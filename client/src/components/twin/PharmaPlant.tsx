import { useMemo } from "react";
import * as THREE from "three";
import { Canvas } from "@react-three/fiber";
import { OrbitControls } from "@react-three/drei";
import { PIPE_COL, Scene, type LiveReadings } from "./Scene";

export interface PharmaPlantProps {
  readings?: {
    reactorTemp?: number;
    pressure?: number;
    wfiFlow?: number;
    yield?: number;
    faults?: number;
  };
  /** canvas height in px (default 440) */
  height?: number;
  /** source badge text, e.g. "● LIVE · SSE" */
  sourceLabel?: string;
}

/** Sensible defaults so the twin renders standalone (mirrors mockup `__live`). */
const DEFAULTS: LiveReadings = {
  reactorTemp: 64,
  pressure: 2,
  wfiFlow: 52,
  yield: 98.4,
  faults: 0,
};

const LEGEND: { label: string; color: number }[] = [
  { label: "Product", color: PIPE_COL.product },
  { label: "WFI", color: PIPE_COL.wfi },
  { label: "Steam", color: PIPE_COL.steam },
  { label: "CIP", color: PIPE_COL.cip },
];

function hex(c: number): string {
  return `#${c.toString(16).padStart(6, "0")}`;
}

/**
 * React-three-fiber port of the approved pharma-plant digital-twin scene:
 * steel skid frame + mezzanine, glass bioreactor with live liquid/glow,
 * WFI/buffer/CIP tanks, filtration bank, fill-finish conveyor, a routed
 * multi-service pipe network, and floating live-reading labels.
 */
export function PharmaPlant({ readings, height = 440, sourceLabel = "● LIVE · SSE" }: PharmaPlantProps) {
  const live: LiveReadings = useMemo(
    () => ({
      reactorTemp: readings?.reactorTemp ?? DEFAULTS.reactorTemp,
      pressure: readings?.pressure ?? DEFAULTS.pressure,
      wfiFlow: readings?.wfiFlow ?? DEFAULTS.wfiFlow,
      yield: readings?.yield ?? DEFAULTS.yield,
      faults: readings?.faults ?? DEFAULTS.faults,
    }),
    [readings?.reactorTemp, readings?.pressure, readings?.wfiFlow, readings?.yield, readings?.faults],
  );

  return (
    <div
      style={{
        position: "relative",
        height,
        borderRadius: 8,
        overflow: "hidden",
        background:
          "radial-gradient(140% 100% at 50% -10%,#1c2c46 0%,#0c1119 58%,#070a0f 100%)",
      }}
    >
      <Canvas
        dpr={[1, 2]}
        camera={{ fov: 40, near: 0.1, far: 120, position: [11, 8, 13] }}
        gl={{ antialias: true, alpha: true }}
        onCreated={({ scene }) => {
          scene.fog = new THREE.Fog(0x0a0f17, 18, 40);
        }}
      >
        <Scene readings={live} />
        <OrbitControls
          enableDamping
          autoRotate
          autoRotateSpeed={0.6}
          enablePan={false}
          minDistance={9}
          maxDistance={26}
          maxPolarAngle={Math.PI / 2.06}
          target={[0, 2.2, 0]}
        />
      </Canvas>

      {/* source badge */}
      <div
        style={{
          position: "absolute",
          left: 12,
          top: 12,
          fontSize: 11,
          fontWeight: 700,
          color: "#3fb950",
          background: "rgba(63,185,80,.12)",
          border: "1px solid rgba(63,185,80,.3)",
          padding: "3px 9px",
          borderRadius: 999,
          pointerEvents: "none",
        }}
      >
        {sourceLabel}
      </div>

      {/* pipe legend */}
      <div
        style={{
          position: "absolute",
          left: 12,
          bottom: 10,
          fontSize: 10.5,
          color: "#94a3b8",
          display: "flex",
          gap: 12,
          flexWrap: "wrap",
          pointerEvents: "none",
        }}
      >
        {LEGEND.map((l) => (
          <span key={l.label} style={{ display: "flex", alignItems: "center", gap: 5 }}>
            <i style={{ width: 9, height: 3, borderRadius: 2, display: "inline-block", background: hex(l.color) }} />
            {l.label}
          </span>
        ))}
      </div>

      {/* hint */}
      <div
        style={{
          position: "absolute",
          right: 12,
          bottom: 10,
          fontSize: 10.5,
          color: "#64748b",
          pointerEvents: "none",
        }}
      >
        drag to orbit · scroll to zoom
      </div>
    </div>
  );
}

export default PharmaPlant;
