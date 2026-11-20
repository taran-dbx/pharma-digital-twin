import { useMemo, useRef } from "react";
import * as THREE from "three";
import { useFrame } from "@react-three/fiber";
import { Html } from "@react-three/drei";
import { DIMS, PIPE_COL, steel, tempColor } from "./materials";
import { ROUTES, flowPosition, type PipeRoute } from "./pipes";

const { FX, FZ, FY, rx, rz, rR, rH } = DIMS;

export interface LiveReadings {
  reactorTemp: number;
  pressure: number;
  wfiFlow: number;
  yield: number;
  faults: number;
}

/* ── shared material singletons (created once) ─────────────── */
const STEEL = steel(0x9aa6b4);
const DARK = steel(0x2a323d, 0.6, 0.6);
const BEAM = steel(0x3a4456, 0.7, 0.45);

/* ── small reusable primitives ─────────────────────────────── */
function Box(props: {
  args: [number, number, number];
  position: [number, number, number];
  material: THREE.Material;
  rotation?: [number, number, number];
}) {
  return (
    <mesh position={props.position} rotation={props.rotation} material={props.material}>
      <boxGeometry args={props.args} />
    </mesh>
  );
}

function Cyl(props: {
  args: [number, number, number, number?];
  position: [number, number, number];
  material: THREE.Material;
  rotation?: [number, number, number];
}) {
  const [r1, r2, h, seg = 28] = props.args;
  return (
    <mesh position={props.position} rotation={props.rotation} material={props.material}>
      <cylinderGeometry args={[r1, r2, h, seg]} />
    </mesh>
  );
}

/* ── structural skid frame (2 levels) ──────────────────────── */
function Frame() {
  const columns: [number, number][] = [
    [-FX, -FZ],
    [FX, -FZ],
    [-FX, FZ],
    [FX, FZ],
    [0, -FZ],
    [0, FZ],
  ];
  const beamYs = [FY, FY * 0.55];
  return (
    <group>
      {columns.map(([x, z], i) => (
        <Box key={`col-${i}`} args={[0.22, FY, 0.22]} position={[x, FY / 2, z]} material={BEAM} />
      ))}
      {beamYs.map((h) => (
        <group key={`beams-${h}`}>
          {[-FZ, FZ, 0].map((z, i) => (
            <Box key={`bx-${i}`} args={[FX * 2, 0.18, 0.18]} position={[0, h, z]} material={BEAM} />
          ))}
          {[-FX, FX, 0].map((x, i) => (
            <Box key={`bz-${i}`} args={[0.18, 0.18, FZ * 2]} position={[x, h, 0]} material={BEAM} />
          ))}
        </group>
      ))}
    </group>
  );
}

/* ── mezzanine deck + railings + cable tray + cabinet ──────── */
function Mezzanine() {
  const deckMat = useMemo(() => steel(0x1a2330, 0.5, 0.7), []);
  const cabinetMat = useMemo(() => steel(0x33424f), []);
  const trayMat = useMemo(() => steel(0x4b5563, 0.5, 0.6), []);
  const hmiMat = useMemo(
    () =>
      new THREE.MeshStandardMaterial({
        color: 0x0c1119,
        emissive: 0x1e90ff,
        emissiveIntensity: 0.5,
      }),
    [],
  );
  const posts: number[] = [];
  for (let i = -FX; i <= FX; i += 1.3) posts.push(i);
  const deckY = FY * 0.55;
  return (
    <group>
      <Box args={[FX * 2 - 0.3, 0.12, FZ * 2 - 0.3]} position={[0, deckY, 0]} material={deckMat} />
      {posts.map((i) => (
        <group key={`rail-${i}`}>
          <Box args={[0.06, 0.5, 0.06]} position={[i, deckY + 0.3, FZ - 0.1]} material={STEEL} />
          <Box args={[0.06, 0.5, 0.06]} position={[i, deckY + 0.3, -FZ + 0.1]} material={STEEL} />
        </group>
      ))}
      <Box args={[FX * 2, 0.05, 0.05]} position={[0, deckY + 0.55, FZ - 0.1]} material={STEEL} />
      <Box args={[FX * 2, 0.05, 0.05]} position={[0, deckY + 0.55, -FZ + 0.1]} material={STEEL} />
      <Box args={[FX * 2 - 0.5, 0.14, 0.4]} position={[0, FY - 0.25, FZ - 0.4]} material={trayMat} />
      <Box args={[1.1, 2, 0.7]} position={[-FX + 0.3, 1.0, FZ - 0.6]} material={cabinetMat} />
      <Box args={[0.7, 0.5, 0.05]} position={[-FX + 0.3, 1.4, FZ - 0.95]} material={hmiMat} />
    </group>
  );
}

/* ── floor + grid ──────────────────────────────────────────── */
function Floor() {
  const floorMat = useMemo(() => steel(0x121925, 0.4, 0.7), []);
  return (
    <group>
      <mesh position={[0, -0.2, 0]} material={floorMat}>
        <boxGeometry args={[16, 0.4, 11]} />
      </mesh>
      <gridHelper args={[16, 32, 0x223047, 0x141c28]} position={[0, 0.02, 0]} />
    </group>
  );
}

/* ── process vessel (tank + dome cap + skirt) ──────────────── */
function Tank({ x, z, r, h, color }: { x: number; z: number; r: number; h: number; color: number }) {
  const mat = useMemo(() => steel(color), [color]);
  return (
    <group>
      <Cyl args={[r, r, h]} position={[x, h / 2 + 0.4, z]} material={mat} />
      <mesh position={[x, h + 0.4, z]} material={mat}>
        <sphereGeometry args={[r, 28, 14, 0, 6.3, 0, Math.PI / 2]} />
      </mesh>
      <Cyl args={[r * 0.5, r * 0.5, 0.4]} position={[x, 0.2, z]} material={DARK} />
    </group>
  );
}

/* ── filtration / heat-exchanger bank ──────────────────────── */
function Filtration() {
  const tubeMat = useMemo(() => steel(0xb6c2cf), []);
  return (
    <group>
      {[0, 1, 2, 3].map((i) => (
        <Cyl
          key={`hx-${i}`}
          args={[0.18, 0.18, 2.2]}
          position={[4.2 + i * 0.42, 1.5, -2.2]}
          material={tubeMat}
          rotation={[0, 0, Math.PI / 2]}
        />
      ))}
      <Box args={[2, 0.3, 1.2]} position={[4.85, 0.45, -2.2]} material={DARK} />
    </group>
  );
}

/* ── central bioreactor (glass + live liquid + stirrer) ────── */
function Bioreactor({ readings }: { readings: LiveReadings }) {
  const liquidRef = useRef<THREE.Mesh>(null);
  const stirRef = useRef<THREE.Group>(null);
  const shaftRef = useRef<THREE.Mesh>(null);
  const lightRef = useRef<THREE.PointLight>(null);

  const glassMat = useMemo(
    () =>
      new THREE.MeshPhysicalMaterial({
        color: 0xbfd3e6,
        roughness: 0.06,
        transparent: true,
        opacity: 0.15,
        side: THREE.DoubleSide,
      }),
    [],
  );
  const liqMat = useMemo(
    () =>
      new THREE.MeshStandardMaterial({
        color: 0x33d27e,
        emissive: 0x0e6b3a,
        emissiveIntensity: 0.7,
        transparent: true,
        opacity: 0.92,
        roughness: 0.25,
      }),
    [],
  );
  const motorMat = useMemo(() => steel(0x3a4452), []);

  useFrame((_, delta) => {
    const t = Math.max(0, Math.min(1, (readings.reactorTemp - 58) / 18));
    const col = tempColor(t);
    const lvl = 1.3 + (readings.wfiFlow - 40) / 25 + Math.sin(performance.now() * 0.002) * 0.04;
    const liquid = liquidRef.current;
    if (liquid) {
      liquid.scale.y = lvl;
      liquid.position.set(rx, 0.55 + lvl / 2, rz);
    }
    liqMat.color.copy(col);
    liqMat.emissive.copy(col).multiplyScalar(0.5);
    liqMat.emissiveIntensity = 0.5 + t * 0.9;
    if (lightRef.current) {
      lightRef.current.color.copy(col);
      lightRef.current.intensity = 0.9 + t * 1.3;
    }
    const spin = delta * 7.5; // ~0.12/frame at 60fps
    if (stirRef.current) stirRef.current.rotation.y += spin;
    if (shaftRef.current) shaftRef.current.rotation.y += spin;
  });

  return (
    <group>
      <mesh position={[rx, rH / 2 + 0.5, rz]} material={glassMat}>
        <cylinderGeometry args={[rR, rR, rH, 48, 1, true]} />
      </mesh>
      {[rH + 0.5, 0.5].map((y, i) => (
        <mesh key={`hoop-${i}`} position={[rx, y, rz]} rotation={[Math.PI / 2, 0, 0]} material={STEEL}>
          <torusGeometry args={[rR, 0.07, 12, 40]} />
        </mesh>
      ))}
      <mesh ref={liquidRef} material={liqMat}>
        <cylinderGeometry args={[rR * 0.93, rR * 0.93, 1, 36]} />
      </mesh>
      <Box args={[0.8, 0.55, 0.8]} position={[rx, rH + 1.05, rz]} material={motorMat} />
      <Cyl args={[0.06, 0.06, rH]} position={[rx, rH / 2 + 0.6, rz]} material={STEEL} />
      {/* invisible spin proxy aligned with the shaft */}
      <mesh ref={shaftRef} position={[rx, rH / 2 + 0.6, rz]} visible={false}>
        <cylinderGeometry args={[0.06, 0.06, rH, 6]} />
      </mesh>
      <group ref={stirRef} position={[rx, 1.1, rz]}>
        {[0, 1, 2].map((i) => (
          <Box
            key={`blade-${i}`}
            args={[rR * 1.1, 0.05, 0.22]}
            position={[0, 0, 0]}
            material={STEEL}
            rotation={[0, (i * Math.PI) / 1.5, 0]}
          />
        ))}
      </group>
      <pointLight ref={lightRef} color={0x33d27e} intensity={1.3} distance={9} position={[rx, 1.9, rz]} />
    </group>
  );
}

/* ── fill-finish conveyor + moving vials + capper ──────────── */
function FillFinish() {
  const vialRefs = useRef<(THREE.Mesh | null)[]>([]);
  const vialMat = useMemo(
    () =>
      new THREE.MeshStandardMaterial({
        color: 0x9fe8ff,
        emissive: 0x0b3b4a,
        emissiveIntensity: 0.5,
        transparent: true,
        opacity: 0.85,
        roughness: 0.2,
      }),
    [],
  );
  const capMat = useMemo(() => steel(0x3a4452), []);

  useFrame((_, delta) => {
    const step = delta * 0.66; // ~0.011/frame at 60fps
    for (const v of vialRefs.current) {
      if (!v) continue;
      v.position.x += step;
      if (v.position.x > 4.5) v.position.x = -0.9;
    }
  });

  return (
    <group>
      <Box args={[5, 0.18, 0.7]} position={[1.6, 0.62, 3.3]} material={DARK} />
      {Array.from({ length: 9 }).map((_, i) => (
        <mesh
          key={`vial-${i}`}
          ref={(el) => (vialRefs.current[i] = el)}
          position={[-0.7 + i * 0.6, 0.9, 3.3]}
          material={vialMat}
        >
          <cylinderGeometry args={[0.11, 0.11, 0.4, 28]} />
        </mesh>
      ))}
      <Box args={[0.4, 1.1, 0.4]} position={[3.0, 1.3, 3.3]} material={capMat} />
      <Box args={[0.5, 0.2, 0.5]} position={[3.0, 0.95, 3.3]} material={STEEL} />
    </group>
  );
}

/* ── pipe network: routed runs + elbows + valves + flow ────── */
function PipeNetwork() {
  const flowRef = useRef<THREE.Mesh>(null);
  const flowRoute = useMemo(() => ROUTES.find((r) => r.withFlow) as PipeRoute, []);

  useFrame(() => {
    if (!flowRef.current || !flowRoute) return;
    const pos = flowPosition(flowRoute, performance.now() * 0.0006);
    flowRef.current.position.set(pos.x, pos.y, pos.z);
  });

  return (
    <group>
      {ROUTES.map((route, ri) => (
        <Route key={`route-${ri}`} route={route} />
      ))}
      {flowRoute && (
        <mesh ref={flowRef}>
          <sphereGeometry args={[0.1, 12, 12]} />
          <meshStandardMaterial color={0x9affc8} emissive={flowRoute.color} emissiveIntensity={1.6} />
        </mesh>
      )}
    </group>
  );
}

function Route({ route }: { route: PipeRoute }) {
  const mat = useMemo(() => steel(route.color, 0.7, 0.4), [route.color]);
  const valveMat = useMemo(() => steel(0xef4444, 0.6, 0.4), []);
  return (
    <group>
      {route.segments.map((seg, i) => (
        <mesh key={`seg-${i}`} position={[seg.mid.x, seg.mid.y, seg.mid.z]} quaternion={seg.quat} material={mat}>
          <cylinderGeometry args={[0.1, 0.1, seg.len, 12]} />
        </mesh>
      ))}
      {route.elbows.map((e, i) => (
        <mesh key={`elbow-${i}`} position={[e.x, e.y, e.z]} material={mat}>
          <sphereGeometry args={[0.14, 14, 14]} />
        </mesh>
      ))}
      <mesh position={[route.valve.x, route.valve.y, route.valve.z]} material={valveMat}>
        <torusGeometry args={[0.16, 0.04, 8, 16]} />
      </mesh>
    </group>
  );
}

/* ── floating equipment labels (drei <Html>) ──────────────── */
function Labels({ readings }: { readings: LiveReadings }) {
  const items: { pos: [number, number, number]; name: string; value: string; unit: string; cls: string }[] = [
    {
      pos: [rx, rH + 1.6, rz],
      name: "Bioreactor",
      value: readings.reactorTemp.toFixed(1),
      unit: " °C",
      cls: "t",
    },
    {
      pos: [-4.5, 3.9, 2.2],
      name: "WFI",
      value: readings.wfiFlow.toFixed(0),
      unit: " L/min",
      cls: "f",
    },
    {
      pos: [-4.5, 3.0, -2.2],
      name: "Buffer",
      value: readings.pressure.toFixed(2),
      unit: " bar",
      cls: "p",
    },
    {
      pos: [1.6, 1.7, 3.3],
      name: "Fill-Finish",
      value: `${readings.yield.toFixed(1)}%`,
      unit: " yield",
      cls: "y",
    },
  ];
  const colorFor: Record<string, string> = {
    t: "#fca5a5",
    f: "#67e8f9",
    p: "#fcd34d",
    y: "#86efac",
  };
  return (
    <>
      {items.map((l, i) => (
        <Html key={`lbl-${i}`} position={l.pos} center distanceFactor={undefined} occlude={false} zIndexRange={[10, 0]}>
          <div
            style={{
              transform: "translateY(-30%)",
              pointerEvents: "none",
              whiteSpace: "nowrap",
              background: "rgba(8,12,20,.74)",
              border: "1px solid rgba(63,185,80,.45)",
              borderRadius: 7,
              padding: "3px 8px",
              fontSize: 11,
              fontWeight: 600,
              color: "#cbd5e1",
              backdropFilter: "blur(3px)",
              boxShadow: "0 2px 10px rgba(0,0,0,.45)",
            }}
          >
            {l.name}{" "}
            <span style={{ fontWeight: 750, color: colorFor[l.cls] }}>{l.value}</span>
            <small style={{ color: "#64748b", fontWeight: 500 }}>{l.unit}</small>
          </div>
        </Html>
      ))}
    </>
  );
}

/** Full 3D scene contents (mounted inside the <Canvas>). */
export function Scene({ readings }: { readings: LiveReadings }) {
  return (
    <>
      <hemisphereLight args={[0x9fc0ff, 0x161e2a, 1.05]} />
      <directionalLight color={0xffffff} intensity={1.4} position={[9, 15, 7]} />
      <directionalLight color={0x88aaff} intensity={0.5} position={[-8, 5, -8]} />
      <Floor />
      <Frame />
      <Mezzanine />
      <Tank x={-4.5} z={2.2} r={0.85} h={3.0} color={0x4a90d9} />
      <Tank x={-4.5} z={-2.2} r={0.8} h={2.4} color={0x6366f1} />
      <Tank x={4.6} z={2.2} r={0.8} h={2.4} color={0xa855f7} />
      <Filtration />
      <Bioreactor readings={readings} />
      <FillFinish />
      <PipeNetwork />
      <Labels readings={readings} />
    </>
  );
}

export { PIPE_COL };
