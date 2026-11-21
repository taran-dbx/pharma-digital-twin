// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { parseField, toDisplay } from "@/components/value/assumptions-panel";
import { DurationHistogram } from "@/components/value/duration-histogram";
import { StatTiles, TilesSkeleton } from "@/components/value/stat-tiles";
import { CellViewTabs } from "@/components/value/value-views";
import type { AssumptionField } from "@/lib/value-api";

afterEach(cleanup);

describe("StatTiles", () => {
  it("renders label, value and foot per tile", () => {
    render(<StatTiles label="Headline" tiles={[{ id: "avoided", label: "Cost avoided to date", value: "$144.8K", foot: "3 of 4" }]} />);
    const tile = screen.getByTestId("value-tile-avoided");
    expect(tile.textContent).toContain("Cost avoided to date");
    expect(tile.textContent).toContain("$144.8K");
    expect(screen.getByRole("list", { name: "Headline" })).toBeTruthy();
  });

  it("marks the loading skeleton busy", () => {
    render(<TilesSkeleton count={3} label="Loading" />);
    expect(screen.getByLabelText("Loading").getAttribute("aria-busy")).toBe("true");
  });
});

describe("DurationHistogram", () => {
  it("shows an empty state without samples", () => {
    render(<DurationHistogram title="Time to detect" samples={[]} caption="c" />);
    expect(screen.getByText("No excursions in this window yet.")).toBeTruthy();
  });

  it("uses the caller's empty state when excursions exist but none were measured", () => {
    render(<DurationHistogram title="Time to acknowledge" samples={[]} caption="c" emptyText="None of 73 excursions acknowledged yet." />);
    expect(screen.getByText("None of 73 excursions acknowledged yet.")).toBeTruthy();
    expect(screen.queryByText("No excursions in this window yet.")).toBeNull();
    expect(screen.queryByRole("button", { name: "Table" })).toBeNull();
  });

  it("shows a tooltip on focus and offers a table view", () => {
    render(<DurationHistogram title="Time to detect" samples={[1, 1, 3]} caption="c" />);
    const cols = screen.getAllByRole("listitem");
    const busiest = cols.find((c) => c.getAttribute("aria-label")?.endsWith(": 2"));
    expect(busiest).toBeTruthy();
    fireEvent.focus(busiest as HTMLElement);
    expect(screen.getByRole("status").textContent).toMatch(/^2/);
    fireEvent.click(screen.getByRole("button", { name: "Table" }));
    expect(screen.getByRole("table")).toBeTruthy();
  });
});

describe("CellViewTabs", () => {
  it("names both personas and switches on click and arrow keys", () => {
    const onChange = vi.fn();
    render(<CellViewTabs value="cell" onChange={onChange} />);
    expect(screen.getByRole("tab", { name: "Executive · Alex Morgan, VP Quality & Site Head" })).toBeTruthy();
    fireEvent.click(screen.getByRole("tab", { name: "Operations · Shreya, Head of Manufacturing Ops" }));
    expect(onChange).toHaveBeenLastCalledWith("ops");
    fireEvent.keyDown(screen.getByRole("tablist"), { key: "ArrowRight" });
    expect(onChange).toHaveBeenLastCalledWith("exec");
  });
});

describe("assumption inputs", () => {
  const capture: AssumptionField = { name: "capture_rate", label: "Capture rate", min: 0, max: 1, integer: false };
  const lines: AssumptionField = { name: "lines_in_scope", label: "Lines", min: 1, max: 1000, integer: true };

  it("edits capture rate as a percentage", () => {
    expect(toDisplay("capture_rate", 0.8)).toBe("80");
    expect(parseField(capture, "75")).toEqual({ value: 0.75 });
    expect(parseField(capture, "120").error).toBe("Between 0% and 100%");
  });

  it("rejects blanks, text and fractions of a whole-number field", () => {
    expect(parseField(lines, "").error).toBe("Enter a number");
    expect(parseField(lines, "abc").error).toBe("Enter a number");
    expect(parseField(lines, "2.5").error).toBe("Whole numbers only");
    expect(parseField(lines, "0").error).toBe("Between 1 and 1,000");
    expect(parseField(lines, "4")).toEqual({ value: 4 });
  });
});
