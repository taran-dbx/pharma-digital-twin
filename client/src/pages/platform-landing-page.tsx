import {
  ArrowRight,
  Check,
  Factory,
  FlaskConical,
  Gauge,
  Radio,
  ShieldCheck,
  TrendingUp,
  Zap,
} from "lucide-react";
import { Link } from "react-router-dom";

const BRAND = "/brand";

type BrandIconProps = {
  file: string;
  className?: string;
};

function BrandIcon({ file, className = "" }: BrandIconProps) {
  return <img className={className} src={`${BRAND}/${file}`} alt="" />;
}

function FlowArrow() {
  return <ArrowRight className="platform-path-arrow" aria-hidden="true" />;
}

function SensorStep() {
  return (
    <div className="platform-path-step">
      <Radio aria-hidden="true" />
      <strong>Edge sensors</strong>
      <small>Sensor readings</small>
    </div>
  );
}

type ProductStepProps = {
  icon: string;
  name: string;
  detail: string;
  tone: "blue" | "coral" | "green";
};

function ProductStep({ icon, name, detail, tone }: ProductStepProps) {
  return (
    <div className={`platform-path-step platform-path-step-${tone}`}>
      <BrandIcon file={icon} />
      <strong>{name}</strong>
      <small>{detail}</small>
    </div>
  );
}

function BrokerStep() {
  return (
    <div className="platform-path-step platform-broker-step">
      <div>
        <span>
          <BrandIcon file="amazon-msk-icon-full-color.svg" />
          <strong>Kafka (MSK)</strong>
        </span>
        <em>or</em>
        <span>
          <BrandIcon file="amazon-kinesis-icon-full-color.svg" />
          <strong>Kinesis</strong>
        </span>
      </div>
      <small>Choose one broker</small>
    </div>
  );
}

function HistoryStep() {
  return (
    <div className="platform-path-step platform-history-step">
      <div>
        <span>
          <BrandIcon file="delta-lake-icon-full-color.svg" />
          <strong>Delta Lake</strong>
        </span>
        <span>
          <BrandIcon file="unity-catalog-icon-full-color.svg" />
          <strong>Unity Catalog</strong>
        </span>
      </div>
      <small>Governed Delta history</small>
    </div>
  );
}

const genieQuestions = [
  "Which lines ran hot last shift?",
  "Compare energy use by asset with last week",
  "Which sensors show increasing drift?",
  "List assets with overdue maintenance",
];

const technologies = [
  ["data-streaming-icon-full-color.svg", "Spark Structured Streaming"],
  ["lakets-icon-full-color.svg", "Lakebase + LakeTS"],
  ["delta-lake-icon-full-color.svg", "Delta Lake"],
  ["unity-catalog-icon-full-color.svg", "Unity Catalog"],
  ["genie-icon-full-color.svg", "Databricks Genie"],
  ["lakeflow-connect-icon-full-color.svg", "Lakeflow Connect"],
] as const;

export function PlatformLandingPage() {
  return (
    <div className="platform-landing">
      <section className="platform-hero">
        <div className="platform-hero-copy">
          <p className="platform-eyebrow">IoT digital twin reference app</p>
          <h1>
            Trace sensor data
            <br />
            from edge to twin.
          </h1>
          <p className="platform-lede">
            This app shows three ingest paths: a sub-second Spark path into Lakebase and LakeTS,
            plus two near-real-time paths into Delta tables governed by Unity Catalog.
          </p>
          <Link className="btn primary platform-hero-cta" to="/industries">
            Choose an industry
            <ArrowRight aria-hidden="true" />
          </Link>
          <div className="platform-scope" aria-label="Platform scope">
            <span>
              <i />
              Lakebase hot path
            </span>
            <span>Delta history</span>
            <span>Three ingest paths</span>
          </div>
        </div>

        <aside className="platform-signal-map" aria-labelledby="signal-map-title">
          <p id="signal-map-title">Three ingestion paths</p>
          <ul>
            <li>
              <i className="platform-signal-dot platform-signal-dot-blue" />
              <span>
                <b className="platform-latency platform-latency-hot">SUB-SECOND TARGET</b>
                Kafka (MSK) or Kinesis → Spark Structured Streaming → Lakebase + LakeTS hot twin
                and app SSE
              </span>
            </li>
            <li>
              <i className="platform-signal-dot platform-signal-dot-blue" />
              <span>
                <b className="platform-latency platform-latency-near">NEAR REAL-TIME</b>
                Kafka (MSK) or Kinesis → Spark Structured Streaming micro-batch → Delta / Unity
                Catalog
              </span>
            </li>
            <li>
              <i className="platform-signal-dot platform-signal-dot-pink" />
              <span>
                <b className="platform-latency platform-latency-near">NEAR REAL-TIME</b>
                Zerobus SDK → Delta / Unity Catalog
              </span>
            </li>
          </ul>
          <p className="platform-signal-foot">
            Change Data Feed moves aged Lakebase rows into governed Delta history.
          </p>
        </aside>
      </section>

      <section className="platform-section" aria-labelledby="platform-flow-title">
        <header className="platform-section-heading">
          <div>
            <p className="platform-eyebrow">The platform flow</p>
            <h2 id="platform-flow-title">Three ingestion paths, one governed history</h2>
          </div>
          <p>
            Only the real-time Spark path writes to the low-latency twin. The micro-batch and
            Zerobus paths write directly to governed Delta history.
          </p>
        </header>

        <ol className="platform-path-list" aria-label="Sensor data architecture paths">
          <li>
            <div className="platform-path-label">
              <span>PATH 1</span>
              <strong>Spark Structured Streaming — real-time</strong>
              <b className="platform-latency platform-latency-hot">SUB-SECOND TARGET</b>
            </div>
            <div className="platform-path-steps">
              <SensorStep />
              <FlowArrow />
              <BrokerStep />
              <FlowArrow />
              <ProductStep
                icon="data-streaming-icon-full-color.svg"
                name="Spark Structured Streaming"
                detail="Continuous processing"
                tone="blue"
              />
              <FlowArrow />
              <ProductStep
                icon="lakets-icon-full-color.svg"
                name="Lakebase + LakeTS"
                detail="Sub-second hot twin"
                tone="green"
              />
            </div>
            <p className="platform-path-note">
              The Spark consumer also sends batches to the app over SSE. Lakebase Change Data Feed
              moves aged rows to Delta tables in Unity Catalog.
            </p>
          </li>

          <li>
            <div className="platform-path-label">
              <span>PATH 2</span>
              <strong>Spark Structured Streaming — micro-batch</strong>
              <b className="platform-latency platform-latency-near">NEAR REAL-TIME</b>
            </div>
            <div className="platform-path-steps">
              <SensorStep />
              <FlowArrow />
              <BrokerStep />
              <FlowArrow />
              <ProductStep
                icon="data-streaming-icon-full-color.svg"
                name="Spark Structured Streaming"
                detail="Scheduled micro-batches"
                tone="blue"
              />
              <FlowArrow />
              <HistoryStep />
            </div>
            <p className="platform-path-note">
              This path skips the hot store and writes directly to Delta.
            </p>
          </li>

          <li>
            <div className="platform-path-label">
              <span>PATH 3</span>
              <strong>Zerobus SDK — direct ingest</strong>
              <b className="platform-latency platform-latency-near">NEAR REAL-TIME</b>
            </div>
            <div className="platform-path-steps">
              <SensorStep />
              <FlowArrow />
              <ProductStep
                icon="lakeflow-connect-icon-full-color.svg"
                name="Zerobus SDK"
                detail="Direct write · no Spark"
                tone="coral"
              />
              <FlowArrow />
              <HistoryStep />
            </div>
            <p className="platform-path-note">
              The SDK writes to Delta without Spark or Lakebase.
            </p>
          </li>
        </ol>

        <aside className="platform-genie" aria-labelledby="platform-genie-title">
          <div className="platform-genie-figure">
            <div className="platform-genie-orb">
              <BrandIcon file="genie-icon-full-color.svg" />
            </div>
            <span>
              <BrandIcon file="genie-icon-full-color.svg" />
              GENIE SPACE
            </span>
          </div>
          <div className="platform-genie-copy">
            <h3 id="platform-genie-title">Query telemetry with Databricks Genie</h3>
            <p>
              Connect a Genie space to the Delta tables in Unity Catalog. Ask about anomalies,
              asset trends, or historical telemetry in plain language, then inspect the generated
              SQL.
            </p>
            <ul>
              <li>
                <ShieldCheck aria-hidden="true" />
                Uses Unity Catalog permissions
              </li>
              <li>
                <Check aria-hidden="true" />
                Keeps generated SQL available for review
              </li>
            </ul>
          </div>
          <div className="platform-genie-samples" aria-label="Example questions for Genie">
            <p>
              <BrandIcon file="genie-icon-full-color.svg" />
              Ask Genie
            </p>
            {genieQuestions.map((question) => (
              <span key={question}>{question}</span>
            ))}
          </div>
        </aside>
      </section>

      <section className="platform-section" aria-labelledby="hot-twin-title">
        <header className="platform-section-heading">
          <div>
            <p className="platform-eyebrow">Hot twin workloads</p>
            <h2 id="hot-twin-title">Where low latency changes the workflow</h2>
          </div>
          <p>The hot path keeps current asset state in Lakebase. Delta retains the history.</p>
        </header>
        <div className="platform-use-cases">
          <article>
            <div>
              <Gauge aria-hidden="true" />
            </div>
            <span>01</span>
            <h3>Failure triage</h3>
            <p>Check current pressure, temperature, and vibration before opening the history.</p>
          </article>
          <article>
            <div>
              <ShieldCheck aria-hidden="true" />
            </div>
            <span>02</span>
            <h3>Process drift</h3>
            <p>Compare the latest reading with operating limits and recent batches.</p>
          </article>
          <article>
            <div>
              <TrendingUp aria-hidden="true" />
            </div>
            <span>03</span>
            <h3>Fleet load</h3>
            <p>See current output and energy use across distributed assets.</p>
          </article>
        </div>
      </section>

      <section className="platform-lakets" aria-labelledby="platform-lakets-title">
        <div className="platform-lakets-copy">
          <p className="platform-eyebrow">
            <BrandIcon file="lakets-icon-full-color.svg" />
            Hot time-series store
          </p>
          <h2 id="platform-lakets-title">
            <BrandIcon file="lakets-icon-full-color.svg" />
            Timeseries Database powered by LakeTS on Lakebase
          </h2>
          <p>
            LakeTS provides ChronoTables, time buckets, gap fill, rate calculations, rollups,
            retention, and tier policies on Lakebase. The toolkit runs in PL/pgSQL, without a
            separate Postgres extension.
          </p>
          <p className="platform-lakets-value">
            The app reads recent sensor state from Lakebase. Change Data Feed moves aged rows into
            governed Delta history.
          </p>
          <a
            className="platform-text-link"
            href="https://databricks-solutions.github.io/lakets/"
            target="_blank"
            rel="noopener noreferrer"
          >
            Explore more with LakeTS
            <ArrowRight aria-hidden="true" />
          </a>
        </div>
        <div className="platform-lakets-capabilities" aria-label="LakeTS capabilities">
          <span>ChronoTables</span>
          <span>1-minute gap fill</span>
          <span>Rate of change</span>
          <span>Value histograms</span>
          <span>Rollups</span>
          <span>Retention</span>
          <span>Partition management</span>
          <span>Hot/cold tier policies</span>
        </div>
      </section>

      <section className="platform-section platform-industries" aria-labelledby="industries-title">
        <header className="platform-section-heading">
          <div>
            <p className="platform-eyebrow">Industry dashboards</p>
            <h2 id="industries-title">Choose a configured digital twin</h2>
          </div>
          <Link className="platform-text-link" to="/industries">
            View industries
            <ArrowRight aria-hidden="true" />
          </Link>
        </header>
        <div className="platform-industry-grid">
          <article>
            <FlaskConical aria-hidden="true" />
            <h3>Pharmaceutical</h3>
            <p>Bioprocessing · Fill-finish</p>
          </article>
          <article>
            <Factory aria-hidden="true" />
            <h3>Manufacturing</h3>
            <p>Assembly · Quality control</p>
          </article>
          <article>
            <Zap aria-hidden="true" />
            <h3>Energy</h3>
            <p>Grid · Turbine telemetry</p>
          </article>
          <article>
            <Gauge aria-hidden="true" />
            <h3>Automotive</h3>
            <p>Paint shop · Final assembly</p>
          </article>
        </div>
      </section>

      <div className="platform-technologies" aria-label="Platform technologies">
        <span>Platform building blocks</span>
        {technologies.map(([icon, name]) => (
          <strong key={name}>
            <BrandIcon file={icon} />
            {name}
          </strong>
        ))}
      </div>
    </div>
  );
}
