import { useEffect, useMemo, useRef, useState } from "react";
import { Canvas, type ThreeEvent, useFrame, useThree } from "@react-three/fiber";
import { ContactShadows, Environment, Html, OrbitControls } from "@react-three/drei";
import * as THREE from "three";
import type { OrbitControls as OrbitControlsImpl } from "three-stdlib";

import "./components/twin/manufacturing-factory.css";

export type StationId = "cnc" | "assembly" | "press" | "compressor" | "hvac" | "qa";
type ThemePreference = "light" | "dark" | "system";
export type ResolvedTheme = "light" | "dark";

interface ScenePalette {
  background: string;
  floor: string;
  gridMajor: string;
  gridMinor: string;
  frame: string;
  ambientIntensity: number;
  directionalIntensity: number;
  environmentIntensity: number;
  skylightIntensity: number;
  shadowOpacity: number;
}

const THEME_OPTIONS: readonly ThemePreference[] = ["light", "dark", "system"];
const SCENE_PALETTES: Record<ResolvedTheme, ScenePalette> = {
  light: {
    background: "#f7f7f7",
    floor: "#ebebeb",
    gridMajor: "#a2a2a2",
    gridMinor: "#d8d8d8",
    frame: "#6f6f6f",
    ambientIntensity: 0.82,
    directionalIntensity: 1.45,
    environmentIntensity: 0.42,
    skylightIntensity: 7,
    shadowOpacity: 0.24,
  },
  dark: {
    background: "#11171c",
    floor: "#1f272d",
    gridMajor: "#445461",
    gridMinor: "#29343b",
    frame: "#5f7281",
    ambientIntensity: 0.42,
    directionalIntensity: 2.1,
    environmentIntensity: 0.32,
    skylightIntensity: 14,
    shadowOpacity: 0.45,
  },
};

export interface LiveReadings {
  throughput: number;
  oee: number;
  spindleSpeed: number;
  spindleTemp: number;
  spindlePower: number;
  coolantFlow: number;
  coolantTemp: number;
  vibration: number;
  lineSpeed: number;
  motorCurrent: number;
  linePosition: number;
  pressPressure: number;
  hydraulicOilTemp: number;
  cycleCount: number;
  compressorPressure: number;
  compressorTemp: number;
  compressorFlow: number;
  zoneTemp: number;
  humidity: number;
  supplyTemp: number;
  returnTemp: number;
  faults: number;
}

interface StationDefinition {
  id: StationId;
  code: string;
  name: string;
  status: string;
  readings: ReadonlyArray<{
    label: string;
    key: keyof LiveReadings;
    unit: string;
    decimals: number;
    derived?: boolean;
  }>;
}

export const STATIONS: Record<StationId, StationDefinition> = {
  cnc: {
    id: "cnc",
    code: "ST-03",
    name: "CNC spindle",
    status: "Cycle active",
    readings: [
      { label: "cnc_1_spindle_speed", key: "spindleSpeed", unit: "RPM", decimals: 0 },
      { label: "cnc_1_spindle_power", key: "spindlePower", unit: "kW", decimals: 1 },
      { label: "cnc_1_spindle_temp", key: "spindleTemp", unit: "°C", decimals: 1 },
      { label: "cnc_1_coolant_flow", key: "coolantFlow", unit: "LPM", decimals: 1 },
      { label: "cnc_1_coolant_temp", key: "coolantTemp", unit: "°C", decimals: 1 },
      { label: "cnc_1_vibration", key: "vibration", unit: "mm/s", decimals: 2 },
    ],
  },
  assembly: {
    id: "assembly",
    code: "AL-01",
    name: "Assembly line",
    status: "Nominal",
    readings: [
      { label: "assy_line_1_speed", key: "lineSpeed", unit: "m/min", decimals: 1 },
      { label: "assy_line_1_motor_current", key: "motorCurrent", unit: "A", decimals: 1 },
      { label: "assy_line_1_position", key: "linePosition", unit: "m", decimals: 1 },
      { label: "Derived throughput", key: "throughput", unit: "u/hr", decimals: 0, derived: true },
    ],
  },
  press: {
    id: "press",
    code: "HP-01",
    name: "Hydraulic press",
    status: "Forming",
    readings: [
      { label: "press_1_pressure", key: "pressPressure", unit: "bar", decimals: 0 },
      { label: "press_1_hydraulic_oil_temp", key: "hydraulicOilTemp", unit: "°C", decimals: 1 },
      { label: "press_1_cycle_count", key: "cycleCount", unit: "count", decimals: 0 },
    ],
  },
  compressor: {
    id: "compressor",
    code: "AC-01",
    name: "Air compressor",
    status: "Loaded",
    readings: [
      { label: "compressor_1_pressure", key: "compressorPressure", unit: "PSI", decimals: 1 },
      { label: "compressor_1_flow", key: "compressorFlow", unit: "CFM", decimals: 0 },
      { label: "compressor_1_motor_temp", key: "compressorTemp", unit: "°C", decimals: 1 },
    ],
  },
  hvac: {
    id: "hvac",
    code: "HV-01",
    name: "HVAC / AHU",
    status: "Conditioning",
    readings: [
      { label: "hvac_zone_1_temp", key: "zoneTemp", unit: "°C", decimals: 1 },
      { label: "hvac_zone_1_humidity", key: "humidity", unit: "%RH", decimals: 0 },
      { label: "hvac_ahu_1_supply_temp", key: "supplyTemp", unit: "°C", decimals: 1 },
      { label: "hvac_ahu_1_return_temp", key: "returnTemp", unit: "°C", decimals: 1 },
    ],
  },
  qa: {
    id: "qa",
    code: "QA-04",
    name: "Vision inspection",
    status: "Derived quality view",
    readings: [
      { label: "Derived first-pass yield", key: "oee", unit: "%", decimals: 1, derived: true },
      { label: "Derived units inspected", key: "throughput", unit: "u/hr", decimals: 0, derived: true },
      { label: "Sensor fault flags", key: "faults", unit: "", decimals: 0, derived: true },
    ],
  },
};

export const INITIAL_READINGS: LiveReadings = {
  throughput: 1204,
  oee: 87.3,
  spindleSpeed: 3500,
  spindleTemp: 45,
  spindlePower: 20,
  coolantFlow: 25,
  coolantTemp: 22,
  vibration: 2,
  lineSpeed: 8,
  motorCurrent: 12,
  linePosition: 50,
  pressPressure: 200,
  hydraulicOilTemp: 48,
  cycleCount: 123456,
  compressorPressure: 105,
  compressorTemp: 65,
  compressorFlow: 300,
  zoneTemp: 21,
  humidity: 45,
  supplyTemp: 15,
  returnTemp: 22,
  faults: 2,
};

const ORANGE = "#ff3621";
const DARK_STEEL = "#222a2f";
const LIGHT_STEEL = "#9aa7ad";
const SAFETY_YELLOW = "#e7a800";

function jitter(value: number, amount: number): number {
  return value + (Math.random() - 0.5) * amount;
}

function shortSensorName(sensorName: string): string {
  return sensorName
    .replace(/^(cnc_1|assy_line_1|press_1|compressor_1|hvac_(zone|ahu)_1)_/, "")
    .split("_")
    .join(" ");
}

function usePrototypeTheme() {
  const [preference, setPreference] = useState<ThemePreference>(() => {
    try {
      const stored = localStorage.getItem("factory-twin-theme");
      if (stored === "light" || stored === "dark" || stored === "system") return stored;
    } catch {
      // Storage can be blocked in embedded/privacy contexts; system is safe.
    }
    return "system";
  });
  const [systemTheme, setSystemTheme] = useState<ResolvedTheme>(() =>
    window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light",
  );
  const resolvedTheme: ResolvedTheme = preference === "system" ? systemTheme : preference;

  useEffect(() => {
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    const updateSystemTheme = () => setSystemTheme(media.matches ? "dark" : "light");
    updateSystemTheme();
    media.addEventListener("change", updateSystemTheme);
    return () => media.removeEventListener("change", updateSystemTheme);
  }, []);

  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove("light", "dark");
    root.classList.add(resolvedTheme);
    root.setAttribute("data-theme", resolvedTheme);
    root.setAttribute("data-theme-preference", preference);
    root.style.colorScheme = resolvedTheme;
    document
      .getElementById("factory-theme-color")
      ?.setAttribute("content", resolvedTheme === "dark" ? "#11171c" : "#f7f7f7");
    try {
      localStorage.setItem("factory-twin-theme", preference);
    } catch {
      // Theme still applies for this session when persistence is unavailable.
    }
  }, [preference, resolvedTheme]);

  return { preference, resolvedTheme, setPreference };
}

function Box({
  position,
  scale,
  color,
  rotation,
}: {
  position: [number, number, number];
  scale: [number, number, number];
  color: string;
  rotation?: [number, number, number];
}) {
  return (
    <mesh position={position} rotation={rotation} castShadow receiveShadow>
      <boxGeometry args={scale} />
      <meshStandardMaterial color={color} metalness={0.72} roughness={0.32} />
    </mesh>
  );
}

function StationLabel({
  position,
  stationId,
  readings,
}: {
  position: [number, number, number];
  stationId: StationId;
  readings: LiveReadings;
}) {
  const station = STATIONS[stationId];
  return (
    <Html position={position} center distanceFactor={8} zIndexRange={[20, 0]}>
      <div className="manufacturing-factory__label">
        <span>{station.code}</span>
        <strong>{station.name}</strong>
        <div className="manufacturing-factory__kpis">
          {station.readings.slice(0, 3).map((reading) => (
            <div key={reading.label}>
              <small>{shortSensorName(reading.label)}</small>
              <b>
                {readings[reading.key].toFixed(reading.decimals)}
                <i>{reading.unit}</i>
              </b>
            </div>
          ))}
        </div>
      </div>
    </Html>
  );
}

function InteractiveStation({
  id,
  selected,
  onSelect,
  children,
}: {
  id: StationId;
  selected: boolean;
  onSelect: (station: StationId) => void;
  children: React.ReactNode;
}) {
  const { gl } = useThree();

  useEffect(
    () => () => {
      gl.domElement.style.cursor = "";
    },
    [gl],
  );

  const handleClick = (event: ThreeEvent<MouseEvent>) => {
    event.stopPropagation();
    onSelect(id);
  };

  return (
    <group
      onClick={handleClick}
      onPointerOver={() => {
        gl.domElement.style.cursor = "pointer";
      }}
      onPointerOut={() => {
        gl.domElement.style.cursor = "";
      }}
    >
      {children}
      {selected && (
        <mesh position={[0, 0.045, 0]} rotation={[-Math.PI / 2, 0, 0]}>
          <ringGeometry args={[2.15, 2.25, 64]} />
          <meshBasicMaterial color={ORANGE} transparent opacity={0.8} />
        </mesh>
      )}
    </group>
  );
}

function FloorGrid({ palette }: { palette: ScenePalette }) {
  const laneMarks = [-12, -6, 0, 6, 12];
  return (
    <group>
      <mesh position={[0, -0.18, 0]} receiveShadow>
        <boxGeometry args={[34, 0.35, 20]} />
        <meshStandardMaterial color={palette.floor} roughness={0.82} metalness={0.12} />
      </mesh>
      <gridHelper args={[34, 68, palette.gridMajor, palette.gridMinor]} position={[0, 0.01, 0]} />
      {laneMarks.map((x) => (
        <group key={x}>
          <Box position={[x, 0.025, -8.8]} scale={[0.13, 0.025, 1.45]} color={SAFETY_YELLOW} />
          <Box position={[x, 0.025, 8.8]} scale={[0.13, 0.025, 1.45]} color={SAFETY_YELLOW} />
        </group>
      ))}
      <Box position={[0, 0.025, 3.25]} scale={[31, 0.025, 0.1]} color="#7e8a91" />
    </group>
  );
}

function BuildingFrame({
  frameColor,
  skylightIntensity,
}: {
  frameColor: string;
  skylightIntensity: number;
}) {
  const xs = [-15, -10, -5, 0, 5, 10, 15];
  return (
    <group>
      {xs.map((x) => (
        <group key={x}>
          <Box position={[x, 4, -9]} scale={[0.18, 8, 0.18]} color={frameColor} />
          <Box position={[x, 4, 9]} scale={[0.18, 8, 0.18]} color={frameColor} />
          <Box position={[x, 7.85, 0]} scale={[0.18, 0.18, 18]} color={frameColor} />
        </group>
      ))}
      <Box position={[0, 7.75, -9]} scale={[30, 0.18, 0.18]} color={frameColor} />
      <Box position={[0, 7.75, 9]} scale={[30, 0.18, 0.18]} color={frameColor} />
      {[-12.5, -7.5, -2.5, 2.5, 7.5, 12.5].map((x) => (
        <mesh key={x} position={[x, 7.5, 0]} rotation={[Math.PI / 2, 0, 0]}>
          <planeGeometry args={[2.8, 0.2]} />
          <meshBasicMaterial color="#d9f3ff" toneMapped={false} />
          <pointLight color="#ccecff" intensity={skylightIntensity} distance={9} position={[0, 0, 0.15]} />
        </mesh>
      ))}
    </group>
  );
}

function Conveyor({ speed, animate }: { speed: number; animate: boolean }) {
  const carrierRefs = useRef<Array<THREE.Group | null>>([]);

  useFrame((_, delta) => {
    if (!animate) return;
    carrierRefs.current.forEach((carrier) => {
      if (!carrier) return;
      carrier.position.x += delta * speed * 0.22;
      if (carrier.position.x > 14.2) carrier.position.x = -14.2;
    });
  });

  return (
    <group position={[0, 0, 0.85]}>
      <Box position={[0, 0.48, 0]} scale={[30, 0.22, 1.1]} color={DARK_STEEL} />
      {[-0.46, 0.46].map((z) => (
        <Box key={z} position={[0, 0.68, z]} scale={[30, 0.13, 0.11]} color={LIGHT_STEEL} />
      ))}
      {Array.from({ length: 10 }, (_, index) => (
        <group
          key={index}
          ref={(element) => {
            carrierRefs.current[index] = element;
          }}
          position={[-13.5 + index * 3, 0.78, 0]}
        >
          <Box position={[0, 0, 0]} scale={[1.15, 0.12, 0.8]} color="#64727a" />
          <Box position={[0, 0.38, 0]} scale={[0.72, 0.62, 0.52]} color={index % 2 === 0 ? ORANGE : "#b9c3c8"} />
        </group>
      ))}
    </group>
  );
}

function CncCell({ readings, animate }: { readings: LiveReadings; animate: boolean }) {
  const spindleRef = useRef<THREE.Mesh>(null);

  useFrame((_, delta) => {
    if (!animate) return;
    if (spindleRef.current) spindleRef.current.rotation.y += delta * (3 + readings.spindleSpeed / 500);
  });

  return (
    <group>
      <Box position={[0, 1.65, 0]} scale={[3.6, 3.25, 3.25]} color="#303a40" />
      <Box position={[0, 1.75, 1.64]} scale={[2.35, 2.1, 0.09]} color="#12191d" />
      <mesh position={[0, 1.75, 1.7]}>
        <planeGeometry args={[2.05, 1.8]} />
        <meshPhysicalMaterial color="#7cb7cc" transparent opacity={0.2} roughness={0.1} metalness={0.1} />
      </mesh>
      <mesh ref={spindleRef} position={[0, 2.5, 1.28]}>
        <cylinderGeometry args={[0.24, 0.14, 1, 24]} />
        <meshStandardMaterial color={LIGHT_STEEL} metalness={0.95} roughness={0.12} />
      </mesh>
      <Box position={[0, 1.12, 1.3]} scale={[1.6, 0.3, 1.25]} color="#11171a" />
      <Box position={[1.48, 2.45, 1.68]} scale={[0.28, 0.64, 0.12]} color={ORANGE} />
      <pointLight color={ORANGE} intensity={readings.spindlePower / 2} distance={4} position={[0, 1.9, 1.7]} />
      <StationLabel position={[0, 4.65, 0]} stationId="cnc" readings={readings} />
    </group>
  );
}

function RobotCell({ animate }: { animate: boolean }) {
  const shoulderRef = useRef<THREE.Group>(null);
  const elbowRef = useRef<THREE.Group>(null);

  useFrame(({ clock }) => {
    if (!animate) return;
    const time = clock.getElapsedTime();
    if (shoulderRef.current) shoulderRef.current.rotation.y = Math.sin(time * 0.65) * 0.58;
    if (elbowRef.current) elbowRef.current.rotation.z = -0.55 + Math.sin(time * 0.9) * 0.22;
  });

  return (
    <group>
      <mesh position={[0, 0.25, 0]} castShadow>
        <cylinderGeometry args={[0.9, 1.1, 0.5, 32]} />
        <meshStandardMaterial color="#252d32" metalness={0.82} roughness={0.28} />
      </mesh>
      <group ref={shoulderRef} position={[0, 0.5, 0]}>
        <mesh position={[0, 0.55, 0]}>
          <cylinderGeometry args={[0.62, 0.72, 1.1, 28]} />
          <meshStandardMaterial color={ORANGE} metalness={0.62} roughness={0.3} />
        </mesh>
        <group rotation={[0, 0, -0.4]} position={[0, 1.1, 0]}>
          <Box position={[0, 1.1, 0]} scale={[0.68, 2.3, 0.68]} color={ORANGE} />
          <mesh position={[0, 2.2, 0]}>
            <sphereGeometry args={[0.48, 24, 24]} />
            <meshStandardMaterial color="#252d32" metalness={0.85} roughness={0.25} />
          </mesh>
          <group ref={elbowRef} position={[0, 2.2, 0]}>
            <Box position={[0, 1.05, 0]} scale={[0.48, 2.2, 0.48]} color={ORANGE} />
            <Box position={[0, 2.18, 0]} scale={[1.15, 0.18, 0.52]} color="#1c2327" />
          </group>
        </group>
      </group>
      {[-2, 2].map((x) => (
        <Box key={x} position={[x, 1.25, 0]} scale={[0.08, 2.5, 4]} color={SAFETY_YELLOW} />
      ))}
    </group>
  );
}

function AssemblyCell({ readings }: { readings: LiveReadings }) {
  return (
    <group>
      <Box position={[0, 1.05, -1.15]} scale={[3.4, 2.05, 0.75]} color="#374249" />
      <Box position={[0, 2.25, -1.15]} scale={[3.65, 0.18, 0.9]} color={LIGHT_STEEL} />
      {[-1.15, 0, 1.15].map((x) => (
        <group key={x} position={[x, 0, 0]}>
          <Box position={[0, 1.0, -0.68]} scale={[0.12, 1.8, 0.12]} color={LIGHT_STEEL} />
          <Box position={[0, 2.7, -0.68]} scale={[0.75, 0.82, 0.12]} color="#11171a" />
          <mesh position={[0, 2.7, -0.6]}>
            <planeGeometry args={[0.55, 0.58]} />
            <meshBasicMaterial color={x === 0 ? ORANGE : "#35d07f"} toneMapped={false} />
          </mesh>
        </group>
      ))}
      <StationLabel position={[0, 4.65, -0.7]} stationId="assembly" readings={readings} />
    </group>
  );
}

function HydraulicPress({ readings, animate }: { readings: LiveReadings; animate: boolean }) {
  const platenRef = useRef<THREE.Group>(null);

  useFrame(({ clock }) => {
    if (!animate || !platenRef.current) return;
    const cycle = (Math.sin(clock.getElapsedTime() * 0.8) + 1) / 2;
    platenRef.current.position.y = 1.65 - cycle * 0.7;
  });

  return (
    <group>
      <Box position={[-1.65, 2.2, 0]} scale={[0.55, 4.4, 3.1]} color="#2d373c" />
      <Box position={[1.65, 2.2, 0]} scale={[0.55, 4.4, 3.1]} color="#2d373c" />
      <Box position={[0, 4.2, 0]} scale={[3.8, 0.55, 3.1]} color="#39464c" />
      <Box position={[0, 0.3, 0]} scale={[3.8, 0.6, 3.1]} color="#1c2327" />
      <group ref={platenRef} position={[0, 1.65, 0]}>
        <Box position={[0, 0, 0]} scale={[2.6, 0.42, 2.1]} color={ORANGE} />
        <Box position={[0, 0.65, 0]} scale={[0.7, 1.3, 0.7]} color={LIGHT_STEEL} />
      </group>
      <Box position={[1.96, 2.6, 1.25]} scale={[0.15, 0.72, 0.55]} color={SAFETY_YELLOW} />
      <StationLabel position={[0, 5.5, 0]} stationId="press" readings={readings} />
    </group>
  );
}

function AirCompressor({ readings, animate }: { readings: LiveReadings; animate: boolean }) {
  const fanRef = useRef<THREE.Mesh>(null);

  useFrame((_, delta) => {
    if (!animate || !fanRef.current) return;
    fanRef.current.rotation.z += delta * (2 + readings.compressorFlow / 45);
  });

  return (
    <group>
      <mesh position={[0, 1.15, 0]} rotation={[0, 0, Math.PI / 2]} castShadow>
        <cylinderGeometry args={[1.05, 1.05, 3.9, 36]} />
        <meshStandardMaterial color="#39464c" metalness={0.78} roughness={0.28} />
      </mesh>
      <Box position={[0, 0.3, 0]} scale={[4.6, 0.35, 1.8]} color="#1c2327" />
      <Box position={[-1.45, 2.65, 0]} scale={[1.5, 2.4, 1.8]} color="#333e44" />
      <mesh ref={fanRef} position={[-1.45, 2.7, 0.92]}>
        <circleGeometry args={[0.58, 8]} />
        <meshStandardMaterial color={ORANGE} metalness={0.55} roughness={0.35} />
      </mesh>
      <mesh position={[1.95, 1.15, 0]}>
        <torusGeometry args={[0.86, 0.1, 12, 36]} />
        <meshStandardMaterial color={SAFETY_YELLOW} metalness={0.6} roughness={0.35} />
      </mesh>
      <StationLabel position={[0, 4.55, 0]} stationId="compressor" readings={readings} />
    </group>
  );
}

function HvacUnit({ readings, animate }: { readings: LiveReadings; animate: boolean }) {
  const fans = useRef<Array<THREE.Group | null>>([]);

  useFrame((_, delta) => {
    if (!animate) return;
    fans.current.forEach((fan) => {
      if (fan) fan.rotation.z += delta * 3.2;
    });
  });

  return (
    <group>
      <Box position={[0, 1.6, 0]} scale={[5.2, 3.2, 2.8]} color="#59666c" />
      {[-1.5, 0, 1.5].map((x, index) => (
        <group
          key={x}
          ref={(element) => {
            fans.current[index] = element;
          }}
          position={[x, 1.75, 1.46]}
        >
          <mesh>
            <circleGeometry args={[0.53, 28]} />
            <meshStandardMaterial color="#141a1d" metalness={0.65} roughness={0.36} />
          </mesh>
          {[0, 1, 2].map((blade) => (
            <Box
              key={blade}
              position={[0, 0, 0.08]}
              scale={[0.12, 0.92, 0.08]}
              color="#9ba8ae"
              rotation={[0, 0, (blade * Math.PI) / 3]}
            />
          ))}
        </group>
      ))}
      <Box position={[0, 3.45, 0]} scale={[4.4, 0.28, 2.2]} color="#303a40" />
      <StationLabel position={[0, 4.8, 0]} stationId="hvac" readings={readings} />
    </group>
  );
}

function QaGate({ readings, animate }: { readings: LiveReadings; animate: boolean }) {
  const scanRef = useRef<THREE.Mesh>(null);

  useFrame(({ clock }) => {
    if (!animate) return;
    if (!scanRef.current) return;
    scanRef.current.position.x = Math.sin(clock.getElapsedTime() * 1.9) * 0.65;
  });

  return (
    <group>
      <Box position={[-1.25, 1.55, 0]} scale={[0.28, 3.1, 2.8]} color="#333f45" />
      <Box position={[1.25, 1.55, 0]} scale={[0.28, 3.1, 2.8]} color="#333f45" />
      <Box position={[0, 3.05, 0]} scale={[2.75, 0.3, 2.8]} color="#333f45" />
      <mesh ref={scanRef} position={[0, 1.75, 0]} rotation={[0, Math.PI / 2, 0]}>
        <planeGeometry args={[2.35, 2.25]} />
        <meshBasicMaterial
          color={readings.faults > 0 ? "#ffb000" : "#35d07f"}
          transparent
          opacity={0.2}
          side={THREE.DoubleSide}
        />
      </mesh>
      <Box position={[1.45, 2.45, 0]} scale={[0.18, 0.52, 0.52]} color={readings.faults > 0 ? SAFETY_YELLOW : "#35d07f"} />
      <StationLabel position={[0, 4.9, 0]} stationId="qa" readings={readings} />
    </group>
  );
}

const CAMERA_FOCUS: Record<StationId, { position: THREE.Vector3; target: THREE.Vector3 }> = {
  cnc: {
    position: new THREE.Vector3(-7, 7.5, 7),
    target: new THREE.Vector3(-11, 1.7, -3.6),
  },
  assembly: {
    position: new THREE.Vector3(3, 8, 10),
    target: new THREE.Vector3(-1.5, 1.6, -2.8),
  },
  press: {
    position: new THREE.Vector3(-1, 8, 13),
    target: new THREE.Vector3(-7.5, 2, 5.6),
  },
  compressor: {
    position: new THREE.Vector3(7, 7, 13),
    target: new THREE.Vector3(1.2, 1.6, 5.7),
  },
  hvac: {
    position: new THREE.Vector3(16, 8, 13),
    target: new THREE.Vector3(9.8, 2, 5.6),
  },
  qa: {
    position: new THREE.Vector3(17, 7, 8),
    target: new THREE.Vector3(11.5, 1.7, -1.6),
  },
};

function FocusedCamera({ selected, animate }: { selected: StationId; animate: boolean }) {
  const controlsRef = useRef<OrbitControlsImpl>(null);
  const lastSelected = useRef<StationId | null>(null);
  const progress = useRef(1);
  const fromPosition = useRef(new THREE.Vector3());
  const fromTarget = useRef(new THREE.Vector3());
  const { camera } = useThree();

  useFrame((_, delta) => {
    const controls = controlsRef.current;
    if (!controls) return;

    if (lastSelected.current !== selected) {
      lastSelected.current = selected;
      progress.current = animate ? 0 : 1;
      fromPosition.current.copy(camera.position);
      fromTarget.current.copy(controls.target);
      if (!animate) {
        camera.position.copy(CAMERA_FOCUS[selected].position);
        controls.target.copy(CAMERA_FOCUS[selected].target);
        controls.update();
      }
    }

    if (!animate && progress.current < 1) {
      progress.current = 1;
      camera.position.copy(CAMERA_FOCUS[selected].position);
      controls.target.copy(CAMERA_FOCUS[selected].target);
      controls.update();
      return;
    }

    if (progress.current < 1) {
      progress.current = Math.min(1, progress.current + delta * 1.55);
      const t = progress.current;
      const eased = t * t * (3 - 2 * t);
      camera.position.lerpVectors(fromPosition.current, CAMERA_FOCUS[selected].position, eased);
      controls.target.lerpVectors(fromTarget.current, CAMERA_FOCUS[selected].target, eased);
      controls.update();
    }
  });

  return (
    <OrbitControls
      ref={controlsRef}
      makeDefault
      enableDamping
      enablePan
      minDistance={6}
      maxDistance={42}
      minPolarAngle={0.35}
      maxPolarAngle={1.42}
    />
  );
}

function FactoryScene({
  readings,
  selected,
  onSelect,
  animate,
  theme,
}: {
  readings: LiveReadings;
  selected: StationId;
  onSelect: (station: StationId) => void;
  animate: boolean;
  theme: ResolvedTheme;
}) {
  const palette = SCENE_PALETTES[theme];
  return (
    <>
      <color attach="background" args={[palette.background]} />
      <fog attach="fog" args={[palette.background, 20, 45]} />
      <ambientLight intensity={palette.ambientIntensity} />
      <directionalLight
        castShadow
        color="#dceeff"
        intensity={palette.directionalIntensity}
        position={[4, 12, 7]}
      />
      <pointLight color={ORANGE} intensity={10} distance={15} position={[-5.6, 5.2, 1]} />
      <FloorGrid palette={palette} />
      <BuildingFrame frameColor={palette.frame} skylightIntensity={palette.skylightIntensity} />
      <Conveyor speed={readings.lineSpeed} animate={animate} />
      <group position={[-11, 0, -3.6]}>
        <InteractiveStation id="cnc" selected={selected === "cnc"} onSelect={onSelect}>
          <CncCell readings={readings} animate={animate} />
        </InteractiveStation>
      </group>
      <group position={[-2, 0, -3]}>
        <InteractiveStation id="assembly" selected={selected === "assembly"} onSelect={onSelect}>
          <RobotCell animate={animate} />
          <group position={[3.3, 0, 0]}>
            <AssemblyCell readings={readings} />
          </group>
        </InteractiveStation>
      </group>
      <group position={[-7.5, 0, 5.6]}>
        <InteractiveStation id="press" selected={selected === "press"} onSelect={onSelect}>
          <HydraulicPress readings={readings} animate={animate} />
        </InteractiveStation>
      </group>
      <group position={[1.2, 0, 5.7]}>
        <InteractiveStation id="compressor" selected={selected === "compressor"} onSelect={onSelect}>
          <AirCompressor readings={readings} animate={animate} />
        </InteractiveStation>
      </group>
      <group position={[9.8, 0, 5.6]}>
        <InteractiveStation id="hvac" selected={selected === "hvac"} onSelect={onSelect}>
          <HvacUnit readings={readings} animate={animate} />
        </InteractiveStation>
      </group>
      <group position={[11.5, 0, -1.6]}>
        <InteractiveStation id="qa" selected={selected === "qa"} onSelect={onSelect}>
          <QaGate readings={readings} animate={animate} />
        </InteractiveStation>
      </group>
      <ContactShadows
        position={[0, 0.03, 0]}
        opacity={palette.shadowOpacity}
        scale={38}
        blur={2.4}
        far={9}
      />
      <Environment preset="warehouse" environmentIntensity={palette.environmentIntensity} />
      <FocusedCamera selected={selected} animate={animate} />
    </>
  );
}

export interface ManufacturingFactoryProps {
  readings?: Partial<LiveReadings>;
  selectedStation?: StationId;
  onStationSelect?: (station: StationId) => void;
  animate?: boolean;
  theme?: ResolvedTheme;
  className?: string;
  showStationControls?: boolean;
}

export function ManufacturingFactory({
  readings,
  selectedStation,
  onStationSelect,
  animate = true,
  theme,
  className,
  showStationControls = true,
}: ManufacturingFactoryProps) {
  const [internalSelected, setInternalSelected] = useState<StationId>("assembly");
  const [documentTheme, setDocumentTheme] = useState<ResolvedTheme>(() =>
    typeof document !== "undefined" &&
    (document.documentElement.dataset.theme === "dark" ||
      document.documentElement.classList.contains("dark"))
      ? "dark"
      : "light",
  );
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(() =>
    typeof window !== "undefined" &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches,
  );
  const selected = selectedStation ?? internalSelected;
  const resolvedTheme = theme ?? documentTheme;
  const mergedReadings = useMemo(() => {
    const definedReadings = Object.fromEntries(
      Object.entries(readings ?? {}).filter(([, value]) => value !== undefined),
    ) as Partial<LiveReadings>;
    return { ...INITIAL_READINGS, ...definedReadings };
  }, [readings]);
  useEffect(() => {
    if (theme || typeof document === "undefined") return;
    const root = document.documentElement;
    const syncTheme = () => {
      setDocumentTheme(
        root.dataset.theme === "dark" || root.classList.contains("dark")
          ? "dark"
          : "light",
      );
    };
    const observer = new MutationObserver(syncTheme);
    observer.observe(root, {
      attributes: true,
      attributeFilter: ["class", "data-theme"],
    });
    syncTheme();
    return () => observer.disconnect();
  }, [theme]);
  useEffect(() => {
    if (typeof window === "undefined") return;
    const media = window.matchMedia("(prefers-reduced-motion: reduce)");
    const syncMotionPreference = () => setPrefersReducedMotion(media.matches);
    syncMotionPreference();
    media.addEventListener("change", syncMotionPreference);
    return () => media.removeEventListener("change", syncMotionPreference);
  }, []);
  const handleSelect = (station: StationId) => {
    if (selectedStation === undefined) setInternalSelected(station);
    onStationSelect?.(station);
  };

  return (
    <div
      className={`manufacturing-factory${className ? ` ${className}` : ""}`}
      role="region"
      aria-label="Interactive 3D manufacturing factory"
      data-scene-theme={resolvedTheme}
      data-motion={animate && !prefersReducedMotion ? "running" : "paused"}
    >
      <Canvas
        shadows
        dpr={[1, 1.75]}
        camera={{ fov: 42, near: 0.1, far: 120, position: [22, 17, 28] }}
        gl={{ antialias: true, alpha: false, toneMapping: THREE.ACESFilmicToneMapping }}
      >
        <FactoryScene
          readings={mergedReadings}
          selected={selected}
          onSelect={handleSelect}
          animate={animate && !prefersReducedMotion}
          theme={resolvedTheme}
        />
      </Canvas>
      <div className="manufacturing-factory__vignette" aria-hidden="true" />
      {showStationControls && (
        <nav className="manufacturing-factory__stations" aria-label="Factory stations">
          {(Object.values(STATIONS) as StationDefinition[]).map((station) => (
            <button
              key={station.id}
              type="button"
              className={selected === station.id ? "active" : ""}
              aria-label={`Focus ${station.name}`}
              aria-pressed={selected === station.id}
              onClick={() => handleSelect(station.id)}
            >
              <span>{station.code}</span>
              {station.name}
            </button>
          ))}
        </nav>
      )}
    </div>
  );
}

function Sparkline({ seed }: { seed: number }) {
  const points = useMemo(
    () =>
      Array.from({ length: 22 }, (_, index) => {
        const wave = Math.sin((index + seed) * 0.72) * 8;
        const fine = Math.cos((index + seed) * 1.6) * 3;
        return `${(index / 21) * 220},${30 - wave - fine}`;
      }).join(" "),
    [seed],
  );

  return (
    <svg viewBox="0 0 220 60" preserveAspectRatio="none" aria-hidden="true">
      <path d={`M ${points.split(" ").join(" L ")}`} />
    </svg>
  );
}

export function FactoryPrototype() {
  const [selected, setSelected] = useState<StationId>("assembly");
  const [readings, setReadings] = useState<LiveReadings>(INITIAL_READINGS);
  const [tick, setTick] = useState(0);
  const [motionPaused, setMotionPaused] = useState(false);
  const { preference, resolvedTheme, setPreference } = usePrototypeTheme();

  useEffect(() => {
    const media = window.matchMedia("(prefers-reduced-motion: reduce)");
    const respectPreference = () => {
      if (media.matches) setMotionPaused(true);
    };
    respectPreference();
    media.addEventListener("change", respectPreference);
    return () => media.removeEventListener("change", respectPreference);
  }, []);

  useEffect(() => {
    const interval = window.setInterval(() => {
      setReadings((current) => ({
        ...current,
        throughput: Math.round(jitter(1204, 34)),
        oee: jitter(87.3, 0.9),
        spindleSpeed: jitter(3500, 100),
        spindleTemp: jitter(45, 4),
        spindlePower: jitter(20, 3),
        coolantFlow: jitter(25, 2),
        coolantTemp: jitter(22, 1),
        vibration: jitter(2, 0.5),
        lineSpeed: jitter(8, 0.4),
        motorCurrent: jitter(12, 1),
        linePosition: (current.linePosition + 0.8) % 100,
        pressPressure: jitter(200, 10),
        hydraulicOilTemp: jitter(48, 3),
        cycleCount: current.cycleCount + 1,
        compressorPressure: jitter(105, 4),
        compressorTemp: jitter(65, 5),
        compressorFlow: jitter(300, 30),
        zoneTemp: jitter(21, 0.6),
        humidity: jitter(45, 4),
        supplyTemp: jitter(15, 1),
        returnTemp: jitter(22, 1),
      }));
      setTick((current) => current + 1);
    }, 1200);
    return () => window.clearInterval(interval);
  }, []);

  const station = STATIONS[selected];

  return (
    <main className="factory-shell">
      <header className="factory-header">
        <div className="wordmark">
          <strong>ForgeLine</strong>
          <span>Digital twin</span>
        </div>
        <div className="header-rule" />
        <div className="line-title">
          <span className="micro-label">Facility / Line 04</span>
          <b>Precision assembly — Stuttgart</b>
        </div>
        <div className="theme-switch" role="radiogroup" aria-label="Color theme">
          {THEME_OPTIONS.map((option) => (
            <button
              key={option}
              type="button"
              className={preference === option ? "active" : ""}
              role="radio"
              aria-checked={preference === option}
              onClick={() => setPreference(option)}
            >
              {option}
            </button>
          ))}
        </div>
        <div className="header-live">
          <i className="pulse" aria-hidden="true" />
          SIMULATED TELEMETRY · 1.2 S
          <button
            className="motion-toggle"
            type="button"
            aria-pressed={motionPaused}
            onClick={() => setMotionPaused((current) => !current)}
          >
            {motionPaused ? "Resume motion" : "Pause motion"}
          </button>
        </div>
      </header>

      <section className="factory-main">
        <ManufacturingFactory
          className="scene-wrap"
          readings={readings}
          selectedStation={selected}
          onStationSelect={setSelected}
          animate={!motionPaused}
          theme={resolvedTheme}
          showStationControls={false}
        />

        <div className="scene-help">Select an area to fly in · drag to orbit · scroll to zoom</div>

        <aside className="left-rail">
          <div className="hero-copy">
            <span className="eyebrow">Manufacturing / Live model</span>
            <h1>
              Factory floor <em>04</em>
            </h1>
            <p>
              Explore machining, assembly, forming and plant utilities. Select any area
              to fly in and inspect its generated sensor signals.
            </p>
          </div>
          <div className="rail-metrics">
            <div className="metric">
              <span className="micro-label">Line throughput</span>
              <div className="metric-value">
                {readings.throughput.toLocaleString()} <small>units/hr</small>
              </div>
              <span className="metric-trend">↑ 3.8%</span>
            </div>
            <div className="metric">
              <span className="micro-label">Overall OEE</span>
              <div className="metric-value">
                {readings.oee.toFixed(1)} <small>%</small>
              </div>
              <span className="metric-trend">NOMINAL</span>
            </div>
            <div className="metric">
              <span className="micro-label">Active faults</span>
              <div className="metric-value">
                {readings.faults} <small>advisories</small>
              </div>
              <span className="metric-trend" style={{ color: "#ffb000" }}>REVIEW</span>
            </div>
          </div>
        </aside>

        <nav className="station-strip" aria-label="Factory stations">
          {(Object.values(STATIONS) as StationDefinition[]).map((item) => (
            <button
              className={`station-button ${selected === item.id ? "active" : ""}`}
              key={item.id}
              onClick={() => setSelected(item.id)}
              aria-pressed={selected === item.id}
              type="button"
            >
              <span>{item.code}</span>
              <b>{item.name}</b>
            </button>
          ))}
        </nav>

        <aside className="inspector">
          <div className="inspector-head">
            <span className="eyebrow">Selected asset / {station.code}</span>
            <h2>{station.name}</h2>
            <div className="status-chip">
              <i className="pulse" aria-hidden="true" />
              {station.status}
            </div>
          </div>
          <div className="inspector-readings">
            {station.readings.map((reading) => (
              <div className="reading" key={reading.label}>
                <span className="reading-name">
                  {reading.label}
                  {reading.derived ? " · derived" : ""}
                </span>
                <span className="reading-value">
                  {readings[reading.key].toFixed(reading.decimals)}
                  <small>{reading.unit}</small>
                </span>
              </div>
            ))}
          </div>
          <div className="sparkline">
            <span className="micro-label">Simulated signal preview</span>
            <Sparkline seed={tick} />
          </div>
        </aside>
      </section>
    </main>
  );
}
