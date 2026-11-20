(function () {
  "use strict";

  const originalFetch = window.fetch;
  if (typeof originalFetch !== "function") {
    return;
  }

  const REFRESH_INTERVAL_MS = 30000;
  const CONTEXT_CHECK_INTERVAL_MS = 1000;
  const TRIGGER_PATH =
    /^\/api\/pipelines\/([^/]+)\/lakets\/(rate|gapfill)\/?$/;
  const SVG_NS = "http://www.w3.org/2000/svg";

  let target = null;
  let contextSection = null;
  let launcher = null;
  let drawer = null;
  let appRoot = null;
  let content = null;
  let refreshButton = null;
  let closeButton = null;
  let refreshTimer = null;
  let requestInFlight = false;
  let lastRequestStartedAt = 0;
  let selectedSensor = null;
  let dashboard = null;
  let activeTab = "overview";
  let statusRegion = null;
  let contextLabel = null;
  let appRootRestore = null;
  let contextTimer = null;
  let contextMountToken = 0;
  const supportsInert =
    typeof HTMLElement !== "undefined" && "inert" in HTMLElement.prototype;
  const MAX_ENHANCE_RETRIES = 8;
  const ENHANCE_RETRY_MS = 150;
  const PHASE_STRIP_SEGMENTS = 48;
  const chartRows = { gapfill: null, rate: null };
  const chartVersion = { gapfill: 0, rate: 0 };
  const renderedChart = {
    gapfill: { version: -1, sensor: null },
    rate: { version: -1, sensor: null },
  };
  const enhanceToken = { gapfill: 0, rate: 0 };
  const chartTitles = {
    gapfill: "time_bucket_gapfill",
    rate: "lakets.rate",
  };

  function requestUrl(input) {
    if (input instanceof Request) {
      return input.url;
    }
    if (input instanceof URL) {
      return input.href;
    }
    return String(input);
  }

  function requestMethod(input, init) {
    if (init && init.method) {
      return String(init.method).toUpperCase();
    }
    if (input instanceof Request) {
      return input.method.toUpperCase();
    }
    return "GET";
  }

  function observedTarget(input, init) {
    try {
      const url = new URL(requestUrl(input), window.location.href);
      if (
        url.origin !== window.location.origin ||
        requestMethod(input, init) !== "GET"
      ) {
        return null;
      }
      const match = TRIGGER_PATH.exec(url.pathname);
      if (!match) {
        return null;
      }
      return {
        pipelineId: decodeURIComponent(match[1]),
        industry: url.searchParams.get("industry"),
        chartType: match[2],
        pageLocation: currentPageLocation(),
      };
    } catch (_error) {
      return null;
    }
  }

  window.fetch = function () {
    const input = arguments[0];
    const init = arguments[1];
    const observed = observedTarget(input, init);
    const responsePromise = originalFetch.apply(this, arguments);
    return responsePromise.then(function (response) {
      try {
        if (response.ok) {
          if (
            observed &&
            observed.pageLocation === currentPageLocation()
          ) {
            observeLakeTs(observed);
            captureChartRows(response, observed);
          }
        }
      } catch (_error) {
        // Enhancement must never disturb the React data path.
      }
      return response;
    });
  };

  function currentPageLocation() {
    return `${window.location.pathname}${window.location.search}${window.location.hash}`;
  }

  function installNavigationGuards() {
    ["pushState", "replaceState"].forEach(function (methodName) {
      const originalMethod = window.history[methodName];
      if (typeof originalMethod !== "function") {
        return;
      }
      window.history[methodName] = function () {
        const result = originalMethod.apply(this, arguments);
        deactivateContext();
        return result;
      };
    });
    window.addEventListener("popstate", deactivateContext);
    window.addEventListener("hashchange", deactivateContext);
  }

  function isCurrentTarget(observed) {
    return (
      !!target &&
      target.pipelineId === observed.pipelineId &&
      target.industry === observed.industry
    );
  }

  function captureChartRows(response, observed) {
    let cloned;
    try {
      cloned = response.clone();
    } catch (_error) {
      // A body that cannot be cloned is left entirely to React.
      return;
    }
    cloned
      .json()
      .then(function (payload) {
        if (
          !isCurrentTarget(observed) ||
          !payload ||
          !Array.isArray(payload.rows)
        ) {
          return;
        }
        chartRows[observed.chartType] = payload.rows.slice();
        chartVersion[observed.chartType] += 1;
        chooseSelectedSensor();
        scheduleEnhancement(observed.chartType);
      })
      .catch(function () {
        // Parsing failure intentionally leaves the original React chart visible.
      });
  }

  function observeLakeTs(nextTarget) {
    const targetChanged =
      !target ||
      target.pipelineId !== nextTarget.pipelineId ||
      target.industry !== nextTarget.industry;
    target = nextTarget;
    mount();
    scheduleContextMount();
    if (targetChanged) {
      dashboard = null;
      selectedSensor = null;
      chartRows.gapfill = null;
      chartRows.rate = null;
      resetChartEnhancements();
      lastRequestStartedAt = 0;
      if (!drawer.hidden) {
        renderLoading();
        requestDashboard(false);
      }
    }
  }

  function scheduleContextMount() {
    const token = (contextMountToken += 1);
    attemptContextMount(token, 0);
  }

  function attemptContextMount(token, attempt) {
    if (token !== contextMountToken || !target) {
      return;
    }
    const anchor = contextAnchor();
    if (!anchor) {
      detachContextSection();
      if (attempt < MAX_ENHANCE_RETRIES) {
        window.setTimeout(function () {
          attemptContextMount(token, attempt + 1);
        }, ENHANCE_RETRY_MS);
      }
      return;
    }
    updateContextIdentity();
    if (contextSection.parentElement !== anchor.parentElement) {
      anchor.parentElement.insertBefore(contextSection, anchor);
    } else if (contextSection.nextElementSibling !== anchor) {
      anchor.parentElement.insertBefore(contextSection, anchor);
    }
    contextSection.hidden = false;
    scheduleContextCheck();
  }

  function contextAnchor() {
    const gapfillCard = chartCard(chartTitles.gapfill);
    const rateCard = chartCard(chartTitles.rate);
    return [gapfillCard, rateCard].find(function (candidate) {
      return (
        candidate &&
        candidate.parentElement &&
        !candidate.closest('[hidden], [aria-hidden="true"]') &&
        candidate.getClientRects().length > 0
      );
    }) || null;
  }

  function scheduleContextCheck() {
    clearContextTimer();
    if (!target || !contextSection || !contextSection.isConnected) {
      return;
    }
    contextTimer = window.setTimeout(function () {
      if (!contextAnchor()) {
        detachContextSection();
        return;
      }
      scheduleContextCheck();
    }, CONTEXT_CHECK_INTERVAL_MS);
  }

  function clearContextTimer() {
    if (contextTimer !== null) {
      window.clearTimeout(contextTimer);
      contextTimer = null;
    }
  }

  function updateContextIdentity() {
    if (!target || !launcher || !contextLabel) {
      return;
    }
    const industry = target.industry ? ` · ${target.industry}` : "";
    contextLabel.textContent = `Pipeline ${target.pipelineId}${industry}`;
    launcher.setAttribute(
      "aria-label",
      `Open LakeTS health for pipeline ${target.pipelineId}${industry}`,
    );
  }

  function detachContextSection() {
    clearContextTimer();
    if (drawer && !drawer.hidden) {
      closeDrawer(false);
    }
    if (contextSection && contextSection.isConnected) {
      contextSection.remove();
    }
  }

  function deactivateContext() {
    contextMountToken += 1;
    target = null;
    dashboard = null;
    selectedSensor = null;
    chartRows.gapfill = null;
    chartRows.rate = null;
    resetChartEnhancements();
    detachContextSection();
  }

  function sensorNames() {
    const names = new Set();
    ["gapfill", "rate"].forEach(function (chartType) {
      const rows = chartRows[chartType];
      if (!Array.isArray(rows)) {
        return;
      }
      rows.forEach(function (row) {
        if (row && row.sensor_name !== null && row.sensor_name !== undefined) {
          names.add(String(row.sensor_name));
        }
      });
    });
    return Array.from(names).sort(function (left, right) {
      return left.localeCompare(right);
    });
  }

  function chooseSelectedSensor() {
    const sensors = sensorNames();
    if (selectedSensor && sensors.includes(selectedSensor)) {
      return;
    }
    selectedSensor =
      sensors.find(function (sensor) {
        return /temperature|temp/i.test(sensor);
      }) ||
      sensors[0] ||
      null;
  }

  function afterFrames(count, callback) {
    if (count <= 0) {
      callback();
      return;
    }
    window.requestAnimationFrame(function () {
      afterFrames(count - 1, callback);
    });
  }

  function scheduleEnhancement(chartType) {
    const token = (enhanceToken[chartType] += 1);
    afterFrames(2, function () {
      attemptEnhancement(chartType, token, 0);
    });
  }

  function rescheduleAllCharts() {
    ["gapfill", "rate"].forEach(function (chartType) {
      if (Array.isArray(chartRows[chartType])) {
        scheduleEnhancement(chartType);
      }
    });
  }

  function attemptEnhancement(chartType, token, attempt) {
    if (token !== enhanceToken[chartType]) {
      return;
    }
    const card = chartCard(chartTitles[chartType]);
    if (!card) {
      if (attempt < MAX_ENHANCE_RETRIES) {
        window.setTimeout(function () {
          attemptEnhancement(chartType, token, attempt + 1);
        }, ENHANCE_RETRY_MS);
      }
      return;
    }
    enhanceCard(card, chartType);
  }

  function chartCard(prefix) {
    const titled = document.querySelector(`[title^="${prefix}"]`);
    let candidate = titled;
    while (candidate && candidate !== document.body) {
      if (candidate.querySelector(".recharts-responsive-container")) {
        return candidate;
      }
      candidate = candidate.parentElement;
    }
    return null;
  }

  function resetChartEnhancements() {
    enhanceToken.gapfill += 1;
    enhanceToken.rate += 1;
    chartVersion.gapfill = 0;
    chartVersion.rate = 0;
    renderedChart.gapfill = { version: -1, sensor: null };
    renderedChart.rate = { version: -1, sensor: null };
    removeChartOverlays();
  }

  function removeChartOverlays() {
    document
      .querySelectorAll("[data-lakets-focused-chart]")
      .forEach(function (overlay) {
        overlay.remove();
      });
    document
      .querySelectorAll(".lakets-focused-react-chart")
      .forEach(function (native) {
        native.classList.remove("lakets-focused-react-chart");
      });
  }

  function enhanceCard(card, chartType) {
    const rows = chartRows[chartType];
    if (!Array.isArray(rows)) {
      return;
    }
    const nativeChart = card.querySelector(".recharts-responsive-container");
    if (!nativeChart) {
      return;
    }
    const existing = card.querySelector(
      `[data-lakets-focused-chart="${chartType}"]`,
    );
    const rendered = renderedChart[chartType];
    const alreadyCurrent =
      existing &&
      nativeChart.classList.contains("lakets-focused-react-chart") &&
      rendered.version === chartVersion[chartType] &&
      rendered.sensor === selectedSensor;
    if (alreadyCurrent) {
      return;
    }

    let overlay = null;
    try {
      overlay = buildFocusedOverlay(chartType, rows);
    } catch (_error) {
      overlay = null;
    }

    if (!overlay) {
      if (existing) {
        existing.remove();
      }
      nativeChart.classList.remove("lakets-focused-react-chart");
      return;
    }

    const focusedId = document.activeElement && document.activeElement.id;
    if (existing) {
      existing.replaceWith(overlay);
    } else {
      card.append(overlay);
    }
    nativeChart.classList.add("lakets-focused-react-chart");
    rendered.version = chartVersion[chartType];
    rendered.sensor = selectedSensor;
    restoreSelectFocus(focusedId);
  }

  function restoreSelectFocus(elementId) {
    if (!elementId) {
      return;
    }
    const restored = document.getElementById(elementId);
    if (restored && typeof restored.focus === "function") {
      restored.focus();
    }
  }

  function buildFocusedOverlay(chartType, rows) {
    const points = numericChartRows(rows, chartType);
    if (!points.length) {
      return null;
    }
    const svg = createFocusedSvg(points, chartType);
    if (!svg) {
      return null;
    }
    const overlay = element("div", "lakets-focused-chart");
    overlay.setAttribute("data-lakets-focused-chart", chartType);
    overlay.append(
      renderSensorControl(chartType),
      svg,
      chartSummary(points, chartType),
    );
    return overlay;
  }

  function renderSensorControl(chartType) {
    const sensors = sensorNames();
    const control = element("div", "lakets-focused-control");
    const label = element("label", "lakets-focused-label", "Sensor");
    const select = element("select", "lakets-focused-select");
    select.id = `lakets-focused-select-${chartType}`;
    select.setAttribute("aria-label", "Focused sensor");
    sensors.forEach(function (sensor) {
      const option = element("option", null, sensor);
      option.value = sensor;
      option.selected = sensor === selectedSensor;
      select.append(option);
    });
    select.disabled = !sensors.length;
    select.addEventListener("change", function () {
      selectedSensor = String(select.value);
      rescheduleAllCharts();
      if (dashboard && drawer && !drawer.hidden) {
        renderDashboard();
      }
    });
    label.append(select);
    const context = element("div", "lakets-focused-context");
    context.append(
      element(
        "span",
        "lakets-focused-count",
        sensors.length ? `1 of ${sensors.length} sensors` : "No sensors",
      ),
      element(
        "span",
        "lakets-focused-helper",
        "Focused view · applies to both charts",
      ),
    );
    control.append(label, context);
    return control;
  }

  function numericChartRows(rows, chartType) {
    const field = chartType === "rate" ? "rate" : "value";
    const points = [];
    rows.forEach(function (row) {
      if (!row || String(row.sensor_name) !== selectedSensor) {
        return;
      }
      const rawValue = row[field] !== undefined ? row[field] : row.value;
      if (rawValue === null || rawValue === undefined || rawValue === "") {
        return;
      }
      const numericValue = Number(rawValue);
      if (!Number.isFinite(numericValue)) {
        return;
      }
      const time = new Date(row.bucket || row.timestamp || row.event_time);
      if (Number.isNaN(time.getTime())) {
        return;
      }
      points.push({
        time: time,
        value: numericValue,
        gapfilled: Boolean(row.gapfilled),
      });
    });
    points.sort(function (left, right) {
      return left.time - right.time;
    });
    return points;
  }

  function svgNode(tag, attributes) {
    const node = document.createElementNS(SVG_NS, tag);
    Object.keys(attributes || {}).forEach(function (name) {
      node.setAttribute(name, String(attributes[name]));
    });
    return node;
  }

  function createFocusedSvg(points, chartType) {
    const width = 760;
    const height = 220;
    const padding = { top: 18, right: 18, bottom: 34, left: 54 };
    const values = points.map(function (point) {
      return point.value;
    });
    let low = Math.min.apply(null, values);
    let high = Math.max.apply(null, values);
    if (chartType === "rate") {
      low = Math.min(low, 0);
      high = Math.max(high, 0);
    }
    const margin = (high - low || Math.abs(high) || 1) * 0.08;
    low -= margin;
    high += margin;
    const spread = high - low || 1;
    const plotWidth = width - padding.left - padding.right;
    const plotHeight = height - padding.top - padding.bottom;
    const singlePoint = points.length === 1;
    const x = function (index) {
      if (singlePoint) {
        return padding.left + plotWidth / 2;
      }
      return padding.left + (index * plotWidth) / (points.length - 1);
    };
    const y = function (value) {
      return padding.top + ((high - value) / spread) * plotHeight;
    };
    const svg = svgNode("svg", {
      viewBox: `0 0 ${width} ${height}`,
      role: "img",
      "aria-label":
        chartType === "rate"
          ? `Rate of change for ${selectedSensor}; values above zero are increasing and values below zero are decreasing.`
          : `Gap-filled trend for ${selectedSensor}; dashed segments and hollow points are gap-filled.`,
    });
    const title = svgNode("title");
    title.textContent =
      chartType === "rate"
        ? `Rate of change for ${selectedSensor}`
        : `Gap-filled trend for ${selectedSensor}`;
    svg.append(title);

    const tickCount = singlePoint ? 1 : Math.min(6, Math.max(4, points.length));
    for (let tick = 0; tick < tickCount; tick += 1) {
      const index = Math.round(
        (tick * (points.length - 1)) / Math.max(tickCount - 1, 1),
      );
      const tickX = x(index);
      const line = svgNode("line", {
        x1: tickX,
        x2: tickX,
        y1: padding.top,
        y2: height - padding.bottom,
        class: "lakets-focused-grid",
      });
      const label = svgNode("text", {
        x: tickX,
        y: height - 11,
        class: "lakets-focused-axis",
        "text-anchor": "middle",
      });
      label.textContent = points[index].time.toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
      });
      svg.append(line, label);
    }
    [low, high].forEach(function (value) {
      const label = svgNode("text", {
        x: padding.left - 8,
        y: y(value) + 4,
        class: "lakets-focused-axis",
        "text-anchor": "end",
      });
      label.textContent = formatDecimal(value);
      svg.append(label);
    });

    const zeroY = y(0);
    if (chartType === "rate") {
      if (!singlePoint) {
        const areaPoints = [
          `${x(0)},${zeroY}`,
          ...points.map(function (point, index) {
            return `${x(index)},${y(point.value)}`;
          }),
          `${x(points.length - 1)},${zeroY}`,
        ];
        svg.append(
          svgNode("polygon", {
            points: areaPoints.join(" "),
            class: "lakets-focused-rate-area",
          }),
        );
      }
      svg.append(
        svgNode("line", {
          x1: padding.left,
          x2: width - padding.right,
          y1: zeroY,
          y2: zeroY,
          class: "lakets-focused-zero",
        }),
      );
    }
    points.forEach(function (point, index) {
      if (index) {
        const previous = points[index - 1];
        svg.append(
          svgNode("line", {
            x1: x(index - 1),
            y1: y(previous.value),
            x2: x(index),
            y2: y(point.value),
            class:
              chartType === "gapfill" &&
              (point.gapfilled || previous.gapfilled)
                ? "lakets-focused-series lakets-focused-gapfilled"
                : "lakets-focused-series",
          }),
        );
      }
      const marker = svgNode("circle", {
        cx: x(index),
        cy: y(point.value),
        r: point.gapfilled ? 4 : 2.5,
        class: point.gapfilled
          ? "lakets-focused-point lakets-focused-point-gapfilled"
          : "lakets-focused-point",
      });
      const tooltip = svgNode("title");
      tooltip.textContent =
        `${formatTimestamp(point.time)} · ${formatDecimal(point.value)}` +
        (point.gapfilled ? " · gap-filled" : "");
      marker.append(tooltip);
      svg.append(marker);
    });
    return svg;
  }

  function chartSummary(points, chartType) {
    const values = points.map(function (point) {
      return point.value;
    });
    const summary = definitionList();
    summary.classList.add("lakets-focused-summary");
    summary.append(
      metric("Min", Math.min.apply(null, values), formatDecimal),
      metric("Current", values[values.length - 1], formatDecimal),
      metric("Max", Math.max.apply(null, values), formatDecimal),
    );
    if (chartType === "rate") {
      const interpretation = element(
        "p",
        "lakets-focused-interpretation",
        "Above zero: increasing · Below zero: decreasing",
      );
      const wrapper = element("div");
      wrapper.append(summary, interpretation);
      return wrapper;
    }
    return summary;
  }

  function element(tag, className, text) {
    const node = document.createElement(tag);
    if (className) {
      node.className = className;
    }
    if (text !== undefined) {
      node.textContent = text;
    }
    return node;
  }

  function metric(label, value, formatter) {
    const item = element("div", "lakets-obs-metric");
    item.append(
      element("dt", "lakets-obs-metric-label", label),
      element(
        "dd",
        "lakets-obs-metric-value",
        formatValue(value, formatter),
      ),
    );
    return item;
  }

  function formatValue(value, formatter) {
    if (value === null || value === undefined || value === "") {
      return "Unavailable";
    }
    return formatter ? formatter(value) : String(value);
  }

  function formatNumber(value) {
    const number = Number(value);
    return Number.isFinite(number)
      ? new Intl.NumberFormat().format(number)
      : String(value);
  }

  function formatDecimal(value, suffix) {
    const number = Number(value);
    return Number.isFinite(number)
      ? `${new Intl.NumberFormat(undefined, {
          maximumFractionDigits: 2,
        }).format(number)}${suffix || ""}`
      : String(value);
  }

  function formatDuration(value) {
    const seconds = Number(value);
    if (!Number.isFinite(seconds)) {
      return String(value);
    }
    if (seconds < 1) {
      return `${Math.round(seconds * 1000)} ms`;
    }
    if (seconds < 60) {
      return `${formatDecimal(seconds)} s`;
    }
    if (seconds < 3600) {
      return `${formatDecimal(seconds / 60)} min`;
    }
    return `${formatDecimal(seconds / 3600)} h`;
  }

  function formatBytes(value) {
    const bytes = Number(value);
    if (!Number.isFinite(bytes)) {
      return String(value);
    }
    const units = ["B", "KB", "MB", "GB", "TB"];
    let amount = bytes;
    let unit = 0;
    while (Math.abs(amount) >= 1024 && unit < units.length - 1) {
      amount /= 1024;
      unit += 1;
    }
    return `${formatDecimal(amount)} ${units[unit]}`;
  }

  function formatTimestamp(value) {
    const date = new Date(value);
    return Number.isNaN(date.getTime())
      ? String(value)
      : date.toLocaleString();
  }

  function mount() {
    if (launcher) {
      return;
    }

    contextSection = element("section", "lakets-obs-context");
    contextSection.setAttribute("data-lakets-obs-context", "");
    contextSection.setAttribute(
      "aria-labelledby",
      "lakets-obs-context-title",
    );
    const contextCopy = element("div", "lakets-obs-context-copy");
    const contextTitle = element(
      "h2",
      "lakets-obs-context-title",
      "Operations / LakeTS health",
    );
    contextTitle.id = "lakets-obs-context-title";
    contextLabel = element("p", "lakets-obs-context-pipeline");
    contextCopy.append(
      contextTitle,
      contextLabel,
      element(
        "p",
        "lakets-obs-context-description",
        "Operational freshness, storage, rollup, and data-quality telemetry.",
      ),
    );
    launcher = element("button", "lakets-obs-launcher", "View health");
    launcher.type = "button";
    launcher.setAttribute("data-lakets-obs-launcher", "");
    launcher.setAttribute("aria-controls", "lakets-obs-drawer");
    launcher.setAttribute("aria-expanded", "false");
    launcher.addEventListener("click", openDrawer);
    contextSection.append(contextCopy, launcher);

    drawer = element("aside", "lakets-obs-drawer");
    drawer.id = "lakets-obs-drawer";
    drawer.hidden = true;
    drawer.setAttribute("data-lakets-obs-drawer", "");
    drawer.setAttribute("aria-label", "LakeTS observability");
    drawer.setAttribute("role", "dialog");
    drawer.setAttribute("aria-modal", "true");

    const header = element("header", "lakets-obs-header");
    const titleGroup = element("div", "lakets-obs-title-group");
    titleGroup.append(
      element("p", "lakets-obs-eyebrow", "Operational view"),
      element("h2", "lakets-obs-title", "LakeTS observability"),
    );
    const actions = element("div", "lakets-obs-actions");
    refreshButton = element("button", "lakets-obs-button", "Refresh");
    refreshButton.type = "button";
    refreshButton.addEventListener("click", function () {
      requestDashboard(true);
    });
    closeButton = element("button", "lakets-obs-button", "Close");
    closeButton.type = "button";
    closeButton.addEventListener("click", closeDrawer);
    actions.append(refreshButton, closeButton);
    header.append(titleGroup, actions);

    statusRegion = element("p", "lakets-obs-status");
    statusRegion.setAttribute("role", "status");
    statusRegion.setAttribute("aria-live", "polite");
    statusRegion.hidden = true;

    content = element("div", "lakets-obs-content");
    const note = element("details", "lakets-obs-about");
    note.append(
      element("summary", null, "About this view"),
      element(
        "p",
        "lakets-obs-temporary-note",
        "Temporary companion module until the original React source is restored.",
      ),
    );
    drawer.append(header, statusRegion, content, note);
    appRoot = document.getElementById("root");
    document.body.append(drawer);
    document.addEventListener("visibilitychange", handleVisibilityChange);
    document.addEventListener("keydown", handleKeyDown);
  }

  function openDrawer() {
    if (!target || !contextSection.isConnected) {
      return;
    }
    drawer.hidden = false;
    launcher.hidden = true;
    launcher.setAttribute("aria-expanded", "true");
    disableAppRoot();
    if (dashboard) {
      renderDashboard();
    } else {
      renderLoading();
      requestDashboard(false);
    }
    scheduleRefresh();
    closeButton.focus();
  }

  function closeDrawer(restoreFocus) {
    drawer.hidden = true;
    launcher.hidden = false;
    launcher.setAttribute("aria-expanded", "false");
    enableAppRoot();
    clearRefreshTimer();
    if (restoreFocus !== false && launcher.isConnected) {
      launcher.focus();
    }
  }

  function disableAppRoot() {
    if (!appRoot) {
      return;
    }
    if (supportsInert) {
      appRoot.setAttribute("inert", "");
      return;
    }
    appRootRestore = {
      ariaHidden: appRoot.getAttribute("aria-hidden"),
      pointerEvents: appRoot.style.pointerEvents,
    };
    appRoot.setAttribute("aria-hidden", "true");
    appRoot.style.pointerEvents = "none";
  }

  function enableAppRoot() {
    if (!appRoot) {
      return;
    }
    if (supportsInert) {
      appRoot.removeAttribute("inert");
      return;
    }
    if (appRootRestore && appRootRestore.ariaHidden !== null) {
      appRoot.setAttribute("aria-hidden", appRootRestore.ariaHidden);
    } else {
      appRoot.removeAttribute("aria-hidden");
    }
    appRoot.style.pointerEvents = appRootRestore
      ? appRootRestore.pointerEvents
      : "";
    appRootRestore = null;
  }

  function handleVisibilityChange() {
    if (document.hidden) {
      clearRefreshTimer();
    } else if (!drawer.hidden) {
      scheduleRefresh();
    }
  }

  function handleKeyDown(event) {
    if (drawer.hidden) {
      return;
    }
    if (event.key === "Escape") {
      closeDrawer();
      return;
    }
    if (event.key !== "Tab") {
      return;
    }

    const controls = Array.from(
      drawer.querySelectorAll(
        'button:not([disabled]), select:not([disabled]), ' +
          'input:not([disabled]), textarea:not([disabled]), summary, ' +
          '[href], [tabindex]:not([tabindex="-1"])',
      ),
    ).filter(function (control) {
      return !control.closest("[hidden]") && control.offsetParent !== null;
    });
    if (!controls.length) {
      event.preventDefault();
      return;
    }
    const first = controls[0];
    const last = controls[controls.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  function clearRefreshTimer() {
    if (refreshTimer !== null) {
      window.clearTimeout(refreshTimer);
      refreshTimer = null;
    }
  }

  function scheduleRefresh() {
    clearRefreshTimer();
    if (drawer.hidden || document.hidden || !target) {
      return;
    }
    const elapsed = Date.now() - lastRequestStartedAt;
    const delay = Math.max(REFRESH_INTERVAL_MS - elapsed, 0);
    refreshTimer = window.setTimeout(function () {
      requestDashboard(false);
    }, delay);
  }

  async function requestDashboard(forceRefresh) {
    if (!target || requestInFlight) {
      return;
    }
    const elapsed = Date.now() - lastRequestStartedAt;
    if (!forceRefresh && lastRequestStartedAt && elapsed < REFRESH_INTERVAL_MS) {
      scheduleRefresh();
      return;
    }

    requestInFlight = true;
    lastRequestStartedAt = Date.now();
    refreshButton.disabled = true;
    refreshButton.textContent = "Refreshing…";
    if (!dashboard) {
      renderLoading();
    } else {
      setStatus("Refreshing LakeTS telemetry…");
    }

    const requestedTarget = target;
    const query = new URLSearchParams();
    if (requestedTarget.industry) {
      query.set("industry", requestedTarget.industry);
    }
    const suffix = query.toString() ? `?${query}` : "";
    const endpoint =
      `/api/pipelines/${encodeURIComponent(requestedTarget.pipelineId)}` +
      `/lakets/dashboard${suffix}`;

    try {
      const response = await originalFetch.call(window, endpoint, {
        headers: { Accept: "application/json" },
      });
      if (!response.ok) {
        throw new Error(`Dashboard request failed (${response.status})`);
      }
      const payload = await response.json();
      if (isCurrentTarget(requestedTarget)) {
        dashboard = payload;
        renderDashboard();
        setStatus("");
      }
    } catch (error) {
      if (isCurrentTarget(requestedTarget)) {
        renderError(error);
      }
    } finally {
      const targetChangedWhileLoading = !isCurrentTarget(requestedTarget);
      requestInFlight = false;
      refreshButton.disabled = false;
      refreshButton.textContent = "Refresh";
      if (targetChangedWhileLoading) {
        lastRequestStartedAt = 0;
        if (!drawer.hidden) {
          requestDashboard(false);
        }
      } else {
        scheduleRefresh();
      }
    }
  }

  function replaceContent(node) {
    content.replaceChildren(node);
  }

  function setStatus(message) {
    if (!statusRegion) {
      return;
    }
    statusRegion.textContent = message || "";
    statusRegion.hidden = !message;
  }

  function renderLoading() {
    setStatus("Loading LakeTS telemetry…");
    const state = element("div", "lakets-obs-state");
    state.setAttribute("data-lakets-obs-state", "loading");
    state.append(
      element("span", "lakets-obs-spinner"),
      element("p", "lakets-obs-state-title", "Loading LakeTS telemetry…"),
    );
    replaceContent(state);
  }

  function renderError(error) {
    const detail = error instanceof Error ? error.message : String(error);
    setStatus(`Observability request failed: ${detail}`);
    const state = element("div", "lakets-obs-state");
    state.setAttribute("data-lakets-obs-state", "error");
    state.append(
      element("p", "lakets-obs-state-title", "Observability request failed"),
      element("p", "lakets-obs-state-detail", detail),
    );
    replaceContent(state);
  }

  function section(id, title, sectionData, renderer) {
    const card = element("section", "lakets-obs-section");
    card.setAttribute("data-lakets-obs-section", id);
    card.setAttribute("aria-labelledby", `lakets-obs-heading-${id}`);
    const heading = element("h3", "lakets-obs-section-title", title);
    heading.id = `lakets-obs-heading-${id}`;
    card.append(heading);

    if (
      !sectionData ||
      sectionData.value === null ||
      sectionData.value === undefined
    ) {
      const unavailable = element("div", "lakets-obs-unavailable");
      unavailable.setAttribute("data-lakets-obs-state", "unavailable");
      unavailable.append(
        element("strong", "lakets-obs-unavailable-title", "Section unavailable"),
        element(
          "p",
          "lakets-obs-unavailable-reason",
          sectionData && sectionData.reason
            ? String(sectionData.reason)
            : "No telemetry was returned.",
        ),
      );
      card.append(unavailable);
      return card;
    }

    renderer(card, sectionData.value);
    if (sectionData.reason) {
      card.append(
        element("p", "lakets-obs-caveat", String(sectionData.reason)),
      );
    }
    return card;
  }

  function definitionList() {
    return element("dl", "lakets-obs-metrics");
  }

  const TAB_NAMES = ["overview", "storage", "rollup", "quality"];
  const PANEL_ID = "lakets-obs-panel";

  function renderDashboard() {
    const workspace = element("div", "lakets-obs-workspace");
    const tablist = element("div", "lakets-obs-tabs");
    tablist.setAttribute("role", "tablist");
    tablist.setAttribute("aria-label", "LakeTS health views");

    TAB_NAMES.forEach(function (tabName) {
      const tab = element(
        "button",
        "lakets-obs-tab",
        tabName[0].toUpperCase() + tabName.slice(1),
      );
      tab.type = "button";
      tab.id = `lakets-obs-tab-${tabName}`;
      tab.setAttribute("role", "tab");
      tab.setAttribute("aria-controls", PANEL_ID);
      tab.setAttribute("aria-selected", String(activeTab === tabName));
      tab.tabIndex = activeTab === tabName ? 0 : -1;
      tab.addEventListener("click", function () {
        selectTab(tabName);
      });
      tab.addEventListener("keydown", function (event) {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") {
          return;
        }
        event.preventDefault();
        const offset = event.key === "ArrowRight" ? 1 : -1;
        const nextIndex =
          (TAB_NAMES.indexOf(tabName) + offset + TAB_NAMES.length) %
          TAB_NAMES.length;
        selectTab(TAB_NAMES[nextIndex]);
      });
      tablist.append(tab);
    });

    const panel = element("div", "lakets-obs-panel");
    panel.id = PANEL_ID;
    panel.setAttribute("role", "tabpanel");
    panel.setAttribute("aria-labelledby", `lakets-obs-tab-${activeTab}`);
    panel.tabIndex = 0;
    panel.append(renderTab(activeTab));

    workspace.append(tablist, panel);
    replaceContent(workspace);
  }

  function selectTab(tabName) {
    activeTab = tabName;
    renderDashboard();
    const active = document.getElementById(`lakets-obs-tab-${tabName}`);
    if (active) {
      active.focus();
    }
  }

  function renderTab(tabName) {
    const sections = element("div", "lakets-obs-sections");
    if (tabName === "overview") {
      sections.append(renderOverview());
    } else if (tabName === "storage") {
      sections.append(
        section(
          "current-partition",
          "Current partition",
          dashboard.current_partition,
          renderCurrentPartition,
        ),
        section("tiering", "Tier lifecycle", dashboard.tiering, renderTiering),
        section(
          "partition-timeline",
          "Partition timeline",
          dashboard.partitions,
          renderPartitions,
        ),
      );
    } else if (tabName === "rollup") {
      sections.append(
        section(
          "rollup-value-series",
          "Selected-sensor min / avg / max",
          dashboard.rollup,
          renderRollup,
        ),
        section(
          "rollup-refresh",
          "Rollup refresh state",
          dashboard.rollup,
          renderRollupRefresh,
        ),
        section(
          "query-comparison",
          "Raw vs rollup",
          dashboard.query_comparison,
          renderQueryComparison,
        ),
      );
    } else {
      sections.append(
        section(
          "data-completeness",
          "Data completeness",
          dashboard.data_quality,
          renderDataQuality,
        ),
        section("cdf-lag", "CDF", dashboard.cdf, renderCdf),
        section("lvc", "Last Value Cache", dashboard.lvc, renderLvc),
      );
    }
    return sections;
  }

  function sectionValue(sectionData) {
    return sectionData && sectionData.value ? sectionData.value : {};
  }

  function renderOverview() {
    const overview = element("section", "lakets-obs-overview");
    overview.setAttribute("aria-labelledby", "lakets-obs-overview-title");
    const heading = element("div", "lakets-obs-overview-heading");
    const title = element(
      "h3",
      "lakets-obs-overview-title",
      "System at a glance",
    );
    title.id = "lakets-obs-overview-title";
    heading.append(
      title,
      element(
        "p",
        "lakets-obs-overview-copy",
        "Four signals for freshness, storage pressure, and query efficiency.",
      ),
    );
    const cdf = sectionValue(dashboard.cdf);
    const comparison = sectionValue(dashboard.query_comparison);
    const current = sectionValue(dashboard.current_partition);
    const lvc = sectionValue(dashboard.lvc);
    const rawMs = Number(comparison.raw_query_ms);
    const rollupMs = Number(comparison.rollup_query_ms);
    const speedup =
      Number.isFinite(rawMs) && Number.isFinite(rollupMs) && rollupMs > 0
        ? rawMs / rollupMs
        : null;
    const kpis = definitionList();
    kpis.classList.add("lakets-obs-kpi-band");
    kpis.append(
      metric(
        "CDF event lag",
        cdf.hot_to_shadow_event_lag_seconds,
        formatDuration,
      ),
      metric("Rollup query speedup", speedup, function (value) {
        return `${formatDecimal(value)}×`;
      }),
      metric("Current partition rows", current.row_count, formatNumber),
      metric("LVC freshness", lvc.cache_update_age_seconds, formatDuration),
    );
    const status = element("p", "lakets-obs-status-line");
    status.textContent =
      "Use Storage for partition lifecycle, Rollup for query detail, and Quality for freshness diagnostics.";
    overview.append(heading, kpis, status);
    return overview;
  }

  function renderPartitions(card, value) {
    const counts = definitionList();
    const partitionCounts = value.counts || {};
    counts.append(
      metric("Total", partitionCounts.total, formatNumber),
      metric("Active", partitionCounts.active, formatNumber),
      metric("Tiered", partitionCounts.tiered, formatNumber),
      metric("Dropped", partitionCounts.dropped, formatNumber),
    );
    card.append(counts);

    const legend = element("p", "lakets-obs-timeline-legend");
    ["Past", "Current", "Future"].forEach(function (phase) {
      const label = element("span", "lakets-obs-timeline-legend-item", phase);
      label.setAttribute("data-lakets-obs-phase", phase.toLowerCase());
      legend.append(label);
    });
    card.append(legend);

    const partitions = Array.isArray(value.timeline) ? value.timeline : [];
    if (!partitions.length) {
      card.append(element("p", "lakets-obs-empty", "No partitions returned."));
      return;
    }
    card.append(buildPhaseStrip(partitions));

    const currentPartition = sectionValue(dashboard.current_partition);
    let currentIndex = partitions.findIndex(function (partition) {
      return String(partition.phase).toLowerCase() === "current";
    });
    if (currentIndex < 0 && currentPartition.name) {
      currentIndex = partitions.findIndex(function (partition) {
        return String(partition.name) === String(currentPartition.name);
      });
    }
    if (currentIndex < 0) {
      currentIndex = partitions.findIndex(function (partition) {
        return String(partition.phase).toLowerCase() === "future";
      });
    }
    const anchor =
      currentIndex >= 0 ? currentIndex : Math.max(partitions.length - 1, 0);
    const nearby = partitions.slice(
      Math.max(anchor - 3, 0),
      Math.min(anchor + 4, partitions.length),
    );
    const compact = element("ol", "lakets-obs-timeline lakets-obs-timeline-compact");
    nearby.forEach(function (partition) {
      compact.append(partitionRow(partition));
    });
    card.append(element("p", "lakets-obs-window-label", "Current partition ±3"), compact);

    card.append(buildInventory(partitions));
  }

  function phaseColor(partition) {
    const phase = String(partition.phase || "").toLowerCase();
    if (phase === "current") {
      return "var(--lakets-obs-current)";
    }
    if (phase === "future") {
      return "var(--lakets-obs-future)";
    }
    return "var(--lakets-obs-past)";
  }

  function buildPhaseStrip(partitions) {
    const strip = element("div", "lakets-obs-phase-strip");
    strip.setAttribute("role", "img");
    const total = partitions.length;
    const segments = Math.min(total, PHASE_STRIP_SEGMENTS);
    const stops = [];
    for (let index = 0; index < segments; index += 1) {
      const partition = partitions[Math.floor((index * total) / segments)];
      const from = ((index / segments) * 100).toFixed(2);
      const to = (((index + 1) / segments) * 100).toFixed(2);
      stops.push(`${phaseColor(partition)} ${from}% ${to}%`);
    }
    strip.style.backgroundImage = `linear-gradient(to right, ${stops.join(", ")})`;
    const label = `Partition phases across ${formatNumber(total)} partitions, oldest to newest`;
    strip.title = label;
    strip.setAttribute("aria-label", label);
    return strip;
  }

  function buildInventory(partitions) {
    const inventory = element("details", "lakets-obs-inventory");
    inventory.append(
      element("summary", null, `Full partition inventory (${partitions.length})`),
    );
    let built = false;
    inventory.addEventListener("toggle", function () {
      if (!inventory.open || built) {
        return;
      }
      built = true;
      const fullTimeline = element(
        "ol",
        "lakets-obs-timeline lakets-obs-timeline-full",
      );
      partitions.forEach(function (partition) {
        fullTimeline.append(partitionRow(partition));
      });
      inventory.append(fullTimeline);
    });
    return inventory;
  }

  function partitionRow(partition) {
    const row = element("li", "lakets-obs-partition");
    row.setAttribute("data-lakets-obs-phase", formatValue(partition.phase));
    row.append(
      element("span", "lakets-obs-partition-name", formatValue(partition.name)),
      element(
        "span",
        "lakets-obs-partition-range",
        `${formatValue(partition.range_start, formatTimestamp)} – ` +
          formatValue(partition.range_end, formatTimestamp),
      ),
      element(
        "span",
        "lakets-obs-partition-state",
        `${formatValue(partition.phase)} · ${formatValue(partition.status)}`,
      ),
      element(
        "span",
        "lakets-obs-partition-count",
        `${formatValue(partition.row_count, formatNumber)} rows · ` +
          formatValue(partition.size_bytes, formatBytes),
      ),
    );
    return row;
  }

  function renderCurrentPartition(card, value) {
    const metrics = definitionList();
    metrics.append(
      metric(
        "Interval",
        value.range_start && value.range_end
          ? `${formatTimestamp(value.range_start)} – ${formatTimestamp(value.range_end)}`
          : null,
      ),
      metric("Rows", value.row_count, formatNumber),
      metric("Size", value.size_bytes, formatBytes),
      metric("Age", value.age_seconds, formatDuration),
      metric("Status", value.status),
    );
    card.append(metrics);
  }

  function renderRollup(card, value) {
    const series = Array.isArray(value.series) ? value.series : [];
    const sensors = Array.from(
      new Set(
        series
          .filter(function (sample) {
            return (
              sample.sensor_name !== null && sample.sensor_name !== undefined
            );
          })
          .map(function (sample) {
            return String(sample.sensor_name);
          }),
      ),
    ).sort();
    if (!selectedSensor) {
      selectedSensor =
        sensors.find(function (sensor) {
          return /temperature|temp/i.test(String(sensor));
        }) ||
        sensors[0] ||
        null;
    }

    const label = element(
      "label",
      "lakets-obs-select-label",
      "Sensor series",
    );
    label.htmlFor = "lakets-obs-sensor";
    const select = element("select", "lakets-obs-select");
    select.id = "lakets-obs-sensor";
    select.disabled = !sensors.length;
    const selectableSensors = Array.from(
      new Set(selectedSensor ? sensors.concat([selectedSensor]) : sensors),
    ).sort();
    select.disabled = !selectableSensors.length;
    selectableSensors.forEach(function (sensor) {
      const option = element("option", null, String(sensor));
      option.value = String(sensor);
      option.selected = sensor === selectedSensor;
      select.append(option);
    });
    select.addEventListener("change", function () {
      selectedSensor = String(select.value);
      rescheduleAllCharts();
      renderDashboard();
    });
    card.append(label, select);

    const selected = series.filter(function (sample) {
      return String(sample.sensor_name) === selectedSensor;
    });
    card.append(createRollupChart(selected, selectedSensor));
    const latest = selected[selected.length - 1];
    const sampleCount = selected.reduce(function (sum, sample) {
      const count = Number(sample.sample_count);
      return Number.isFinite(count) ? sum + count : sum;
    }, 0);
    const metrics = definitionList();
    metrics.append(
      metric("Bucket", value.bucket),
      metric("Latest stddev", latest ? latest.stddev_value : null, formatDecimal),
      metric("Samples in range", selected.length ? sampleCount : null, formatNumber),
    );
    card.append(metrics);
  }

  function createRollupChart(samples, sensor) {
    const wrapper = element("div", "lakets-obs-chart");
    wrapper.setAttribute("data-lakets-obs-chart", "rollup-min-avg-max");
    if (!samples.length) {
      wrapper.append(
        element(
          "p",
          "lakets-obs-empty",
          "No 1-minute samples for this sensor.",
        ),
      );
      return wrapper;
    }

    const numericSamples = samples
      .map(function (sample) {
        return {
          sample,
          min: Number(sample.min_value),
          avg: Number(sample.avg_value),
          max: Number(sample.max_value),
        };
      })
      .filter(function (point) {
        return (
          Number.isFinite(point.min) &&
          Number.isFinite(point.avg) &&
          Number.isFinite(point.max)
        );
      });
    if (!numericSamples.length) {
      wrapper.append(
        element("p", "lakets-obs-empty", "Rollup range values are unavailable."),
      );
      return wrapper;
    }

    const width = 560;
    const height = 180;
    const padding = 20;
    const low = Math.min.apply(
      null,
      numericSamples.map(function (point) {
        return point.min;
      }),
    );
    const high = Math.max.apply(
      null,
      numericSamples.map(function (point) {
        return point.max;
      }),
    );
    const spread = high - low || 1;
    const x = function (index) {
      return (
        padding +
        (index * (width - padding * 2)) /
          Math.max(numericSamples.length - 1, 1)
      );
    };
    const y = function (number) {
      return height - padding - ((number - low) / spread) * (height - padding * 2);
    };

    const svg = document.createElementNS(SVG_NS, "svg");
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("role", "img");
    svg.setAttribute(
      "aria-label",
      `Minimum, average, and maximum 1-minute values for ${formatValue(sensor)}`,
    );
    const title = document.createElementNS(SVG_NS, "title");
    title.textContent =
      `1-minute rollup range for ${formatValue(sensor)}. ` +
      `Minimum ${formatDecimal(low)}, maximum ${formatDecimal(high)}.`;
    svg.append(title);

    const averagePoints = [];
    numericSamples.forEach(function (point, index) {
      const range = document.createElementNS(SVG_NS, "line");
      range.setAttribute("x1", x(index));
      range.setAttribute("x2", x(index));
      range.setAttribute("y1", y(point.min));
      range.setAttribute("y2", y(point.max));
      range.setAttribute("class", "lakets-obs-chart-range");
      const tooltip = document.createElementNS(SVG_NS, "title");
      tooltip.textContent =
        `${formatValue(point.sample.bucket, formatTimestamp)}: ` +
        `min ${formatDecimal(point.min)}, avg ${formatDecimal(point.avg)}, ` +
        `max ${formatDecimal(point.max)}, stddev ` +
        `${formatValue(point.sample.stddev_value, formatDecimal)}, ` +
        `${formatValue(point.sample.sample_count, formatNumber)} samples`;
      range.append(tooltip);
      svg.append(range);
      averagePoints.push(`${x(index)},${y(point.avg)}`);
    });
    const average = document.createElementNS(SVG_NS, "polyline");
    average.setAttribute("points", averagePoints.join(" "));
    average.setAttribute("class", "lakets-obs-chart-average");
    average.setAttribute("aria-label", "Average value");
    svg.append(average);
    wrapper.append(svg);
    return wrapper;
  }

  function renderRollupRefresh(card, value) {
    const metrics = definitionList();
    metrics.append(
      metric("Watermark", value.watermark, formatTimestamp),
      metric("Last refresh", value.last_refreshed_at, formatTimestamp),
      metric("Refresh age", value.refresh_age_seconds, formatDuration),
      metric("Pending invalidations", value.pending_invalidations, formatNumber),
    );
    card.append(metrics);
  }

  function renderTiering(card, value) {
    const metrics = definitionList();
    metrics.append(
      metric("Active", value.active, formatNumber),
      metric("Age-eligible", value.age_eligible, formatNumber),
      metric("Durable-eligible", value.durable_eligible, formatNumber),
      metric("Tiered", value.tiered, formatNumber),
      metric("Dropped", value.dropped, formatNumber),
    );
    card.append(metrics);
    const policy = value.policy;
    const policyText = policy
      ? `${formatValue(policy.type)} · ${formatValue(policy.enabled)} · ${JSON.stringify(
          policy.config,
        )}`
      : null;
    const policyBlock = element("p", "lakets-obs-policy");
    policyBlock.append(
      element("strong", "lakets-obs-policy-label", "Policy: "),
      document.createTextNode(formatValue(policyText)),
    );
    card.append(policyBlock);
  }

  function renderQueryComparison(card, value) {
    const metrics = definitionList();
    metrics.append(
      metric("Raw timing", value.raw_query_ms, function (number) {
        return formatDecimal(number, " ms");
      }),
      metric("Rollup timing", value.rollup_query_ms, function (number) {
        return formatDecimal(number, " ms");
      }),
      metric("Raw source rows", value.raw_rows, formatNumber),
      metric("Rollup rows", value.rollup_rows, formatNumber),
      metric(
        "Event-to-rollup reduction",
        value.event_to_rollup_row_reduction_percentage ??
          value.row_reduction_percentage,
        function (number) {
          return formatDecimal(number, "%");
        },
      ),
    );
    card.append(metrics);
    [value.raw_error, value.rollup_error]
      .filter(Boolean)
      .forEach(function (error) {
        card.append(element("p", "lakets-obs-caveat", String(error)));
      });
  }

  function renderDataQuality(card, value) {
    const metrics = definitionList();
    metrics.append(
      metric("Missing buckets", value.missing_bucket_count, formatNumber),
      metric(
        "Missing bucket percentage",
        value.missing_bucket_percentage,
        function (number) {
          return formatDecimal(number, "%");
        },
      ),
      metric("Expected buckets", value.expected_bucket_count, formatNumber),
      metric("Observed buckets", value.observed_bucket_count, formatNumber),
      metric("Gapfilled count", value.gapfilled_count, formatNumber),
      metric(
        "Missing sample percentage",
        value.missing_sample_percentage,
        function (number) {
          return formatDecimal(number, "%");
        },
      ),
    );
    card.append(metrics);
    if (value.gapfilled_count === null || value.gapfilled_count === undefined) {
      card.append(
        element(
          "p",
          "lakets-obs-caveat",
          formatValue(
            value.gapfilled_reason,
            null,
          ),
        ),
      );
    }
    if (
      value.missing_sample_percentage === null ||
      value.missing_sample_percentage === undefined
    ) {
      card.append(
        element(
          "p",
          "lakets-obs-caveat",
          formatValue(value.missing_sample_reason),
        ),
      );
    }
  }

  function renderCdf(card, value) {
    const metrics = definitionList();
    metrics.append(
      metric(
        "Hot-to-shadow event lag",
        value.hot_to_shadow_event_lag_seconds,
        formatDuration,
      ),
      metric("WAL lag", value.wal_lag_bytes, formatBytes),
      metric("Hot latest", value.hot_latest, formatTimestamp),
      metric("Shadow latest", value.shadow_latest, formatTimestamp),
      metric("Sync enabled", value.sync_enabled),
    );
    card.append(metrics);
  }

  function renderLvc(card, value) {
    const metrics = definitionList();
    metrics.append(
      metric("Series count", value.cached_series, formatNumber),
      metric(
        "Event lag",
        value.source_to_cache_event_lag_seconds,
        formatDuration,
      ),
      metric("Cache update age", value.cache_update_age_seconds, formatDuration),
      metric("Cache event time", value.cache_event_time, formatTimestamp),
    );
    card.append(metrics);
  }

  installNavigationGuards();
})();
