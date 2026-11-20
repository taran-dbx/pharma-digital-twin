import { type ElementRef, type ReactNode, useEffect, useMemo, useRef, useState } from "react";
import { Canvas, type ThreeEvent, useFrame, useThree } from "@react-three/fiber";
import { ContactShadows, Environment, Html, Lightformer, OrbitControls, RoundedBox } from "@react-three/drei";
import * as THREE from "three";

import { useResolvedTheme, type ResolvedTheme } from "@/hooks/use-resolved-theme";
import {
  CELL_STATIONS,
  HEALTH_LABEL,
  RESPONSE_STEPS,
  STATION_INFO,
  decisionLabel,
  formatDeviceTime,
  healthTone,
  type CellDecisions,
  type CellSensorGroup,
  type CellStationId,
  type HealthState,
} from "@/lib/manufacturing-story";
import { formatNumber } from "@/lib/utils";

import "./machining-cell.css";

/*
 * Layout follows the cell diagram: three station cards stand behind one conveyor
 * that runs under all of them. MILL carries a gantry with spindles 1-3, the
 * LIMIT gauge sits between MILL and INSPECT, INSPECT has an overhead camera and
 * PACK boxes the parts.
 */
const BELT_TOP = 1.0;
const BELT_START = -8.9;
const BELT_END = 9.0;
const BELT_DEPTH = 1.0;
const CARD_Z = -0.85;
const CARD_TOP = 3.5;
const CARD_DEPTH = 0.16;
const STATION_X: Record<CellStationId, number> = { mill: -5.25, inspect: 1.4, pack: 6.5 };
const STATION_WIDTH: Record<CellStationId, number> = { mill: 6.9, inspect: 3.9, pack: 4.1 };
const SPINDLE_X = [-7.39, -5.25, -3.11] as const;
/** Spindle 3 is the instrumented spindle; the other two are context. */
const HOT_SPINDLE = 2;
const BEAM_Y = 3.75;
const LIMIT_X = -1.3;
const TUBE_BOTTOM = 1.25;
const TUBE_TOP = 3.7;
/** The gauge reads 0 to limit × GAUGE_SPAN, so the limit line sits at 80 % of the tube. */
const GAUGE_SPAN = 1.25;
const GATE_X = STATION_X.pack - STATION_WIDTH.pack / 2 + 0.1;
const PART_PITCH = 1.43;

export type CellView = "cell" | CellStationId;

/** A recorded decision: fly to `view` and ripple the station floor. */
export interface CellCue {
  station: CellStationId;
  view: CellView;
  /** Date.now() when the decision was saved. */
  at: number;
}

/** Production is stopped (machines wind down) or restarting (spinning back up). */
export interface CellStopView {
  phase: "stopped" | "restarting";
  /** When the stop or resume was recorded, epoch seconds. */
  since?: number;
  /** Device time of the last machine reading, epoch seconds. */
  lastReading?: number;
  reason: string;
  resuming: boolean;
  onResume: () => void;
}

interface Framing {
  target: [number, number, number];
  direction: [number, number, number];
  /** Half-width and half-height of the region that must stay in frame. */
  width: number;
  height: number;
}

const FRAMING: Record<CellView, Framing> = {
  cell: { target: [0, 2.25, 0.1], direction: [0, 0.34, 1], width: 10.1, height: 2.9 },
  mill: { target: [-4.5, 2.35, 0], direction: [0.22, 0.3, 1], width: 4.9, height: 2.5 },
  inspect: { target: [0.4, 2.35, 0], direction: [-0.12, 0.3, 1], width: 3.5, height: 2.4 },
  pack: { target: [6.1, 2.15, 0], direction: [-0.3, 0.3, 1], width: 3.9, height: 2.4 },
};

const TONE_COLOR: Record<ReturnType<typeof healthTone>, string> = {
  ok: "#3fb950",
  warn: "#f2a33a",
  crit: "#ff3621",
  idle: "#7d8992",
};

interface ScenePalette {
  card: string;
  cardEdge: string;
  ink: string;
  label: string;
  head: string;
  headLabel: string;
  tool: string;
  part: string;
  roller: string;
  carton: string;
  glass: string;
  cone: string;
  dot: string;
  selection: string;
  shadow: number;
  ambient: number;
  key: number;
  fill: string;
}

const PALETTE: Record<ResolvedTheme, ScenePalette> = {
  dark: {
    card: "#2b3842",
    cardEdge: "#3f505c",
    ink: "#4b5d6a",
    label: "#9fb0bb",
    head: "#9aa7b0",
    headLabel: "#0f171c",
    tool: "#7f9cad",
    part: "#d5dde2",
    roller: "#2a3640",
    carton: "#a5835a",
    glass: "#cfe3ee",
    cone: "#cfe3ee",
    dot: "#3a4954",
    selection: "#e8ecf0",
    shadow: 0.6,
    ambient: 0.55,
    key: 2.3,
    fill: "#7fa3c4",
  },
  light: {
    card: "#ffffff",
    cardEdge: "#e6e3dc",
    ink: "#1f2d33",
    label: "#4c565c",
    head: "#e7e5df",
    headLabel: "#1f2d33",
    tool: "#5b7686",
    part: "#ffffff",
    roller: "#ffffff",
    carton: "#c49a67",
    glass: "#ffffff",
    cone: "#c4d3dc",
    dot: "#c9c4b8",
    selection: "#1f2d33",
    shadow: 0.28,
    ambient: 0.95,
    key: 2.1,
    fill: "#ffffff",
  },
};

export interface StationView {
  health: HealthState;
  /** Short live readout under the station label, e.g. "47.2 °C". */
  readout: string;
}

export interface CellSensorView {
  name: string;
  label: string;
  group: CellSensorGroup;
  value?: number;
  unit: string;
  decimals: number;
  overLimit: boolean;
}

export interface MachiningCellProps {
  stations: Record<CellStationId, StationView>;
  sensors: CellSensorView[];
  selected: CellStationId;
  onSelect: (station: CellStationId) => void;
  decisions: CellDecisions;
  /** The PACK gate is down: a hold is recorded and no resume has released it. */
  batchHeld: boolean;
  /** 0..1 heat on the spindle, from how close the reading is to the limit. */
  spindleHeat: number;
  spindleLimit?: number;
  spindleRpm?: number;
  lineSpeed?: number;
  animate: boolean;
  statusLabel: string;
  expanded?: boolean;
  onToggleExpanded?: () => void;
  /** Set while production is stopped or restarting. */
  stop?: CellStopView | null;
  cue?: CellCue | null;
}

const HOT_X = SPINDLE_X[HOT_SPINDLE];

// Where each sensor physically sits, in world coordinates.
const SENSOR_ANCHORS: Record<string, { at: [number, number, number]; side: "left" | "right" }> = {
  cnc_1_spindle_temp: { at: [HOT_X - 0.2, 3.45, 0.3], side: "left" },
  cnc_1_spindle_speed: { at: [HOT_X + 0.12, 2.25, 0.1], side: "right" },
  cnc_1_spindle_power: { at: [HOT_X + 0.38, 3.35, 0.1], side: "right" },
  cnc_1_vibration: { at: [HOT_X + 0.38, 2.8, 0.1], side: "right" },
  cnc_1_coolant_flow: { at: [HOT_X - 0.33, 2.4, 0.25], side: "left" },
  cnc_1_coolant_temp: { at: [-9.75, 1.2, -0.1], side: "left" },
  assy_line_1_speed: { at: [5.2, BELT_TOP + 0.05, BELT_DEPTH / 2], side: "right" },
  assy_line_1_motor_current: { at: [9.35, 0.6, 0.78], side: "left" },
  compressor_1_pressure: { at: [-9.6, 1.75, -2.3], side: "right" },
  hvac_zone_1_temp: { at: [9.7, 2.75, -1.9], side: "left" },
};

function Matte({
  color,
  roughness = 0.62,
  metalness = 0.08,
  ...rest
}: { color: string; roughness?: number; metalness?: number } & JSX.IntrinsicElements["meshStandardMaterial"]) {
  return <meshStandardMaterial color={color} roughness={roughness} metalness={metalness} {...rest} />;
}

function Block({
  position,
  size,
  color,
  radius = 0.04,
  roughness,
  metalness,
}: {
  position: [number, number, number];
  size: [number, number, number];
  color: string;
  radius?: number;
  roughness?: number;
  metalness?: number;
}) {
  return (
    <RoundedBox args={size} radius={radius} smoothness={4} position={position} castShadow receiveShadow>
      <Matte color={color} roughness={roughness} metalness={metalness} />
    </RoundedBox>
  );
}

interface LabelArt {
  texture: THREE.CanvasTexture;
  aspect: number;
}

const LABEL_PX = 96;

/** Draws `text` into a texture so labels print on the model without a font download. */
function drawLabel(text: string, color: string, tracking: number, weight: number): LabelArt {
  const canvas = document.createElement("canvas");
  const ctx = canvas.getContext("2d");
  const font = `${weight} ${LABEL_PX}px Inter, "Helvetica Neue", Arial, sans-serif`;
  const chars = [...text];
  let widths = chars.map(() => LABEL_PX * 0.6);
  if (ctx) {
    ctx.font = font;
    widths = chars.map((c) => ctx.measureText(c).width);
  }
  const textWidth = widths.reduce((sum, w) => sum + w, 0) + tracking * LABEL_PX * Math.max(0, chars.length - 1);
  canvas.width = Math.ceil(textWidth + LABEL_PX * 0.2);
  canvas.height = Math.ceil(LABEL_PX * 1.3);
  if (ctx) {
    ctx.font = font;
    ctx.fillStyle = color;
    ctx.textBaseline = "middle";
    let x = LABEL_PX * 0.1;
    chars.forEach((c, i) => {
      ctx.fillText(c, x, canvas.height / 2);
      x += widths[i] + tracking * LABEL_PX;
    });
  }
  const texture = new THREE.CanvasTexture(canvas);
  texture.colorSpace = THREE.SRGBColorSpace;
  texture.anisotropy = 4;
  return { texture, aspect: canvas.width / canvas.height };
}

function Label({
  text,
  color,
  height,
  position,
  anchor = "center",
  tracking = 0.14,
  weight = 600,
}: {
  text: string;
  color: string;
  /** World height of the label plane; the glyphs fill about 55 % of it. */
  height: number;
  position: [number, number, number];
  anchor?: "center" | "left";
  tracking?: number;
  weight?: number;
}) {
  const art = useMemo(() => drawLabel(text, color, tracking, weight), [text, color, tracking, weight]);
  useEffect(() => () => art.texture.dispose(), [art]);
  const width = height * art.aspect;
  const x = position[0] + (anchor === "left" ? width / 2 : 0);
  return (
    <mesh position={[x, position[1], position[2]]}>
      <planeGeometry args={[width, height]} />
      <meshBasicMaterial map={art.texture} transparent depthWrite={false} toneMapped={false} />
    </mesh>
  );
}

/** Dotted shop floor that fades out toward the edges, like the cell diagram's backdrop. */
function DotFloor({ palette }: { palette: ScenePalette }) {
  const texture = useMemo(() => {
    const canvas = document.createElement("canvas");
    canvas.width = 1600;
    canvas.height = 800;
    const ctx = canvas.getContext("2d");
    if (ctx) {
      const pitch = 32;
      for (let y = pitch / 2; y < canvas.height; y += pitch) {
        for (let x = pitch / 2; x < canvas.width; x += pitch) {
          const dx = (x - canvas.width / 2) / (canvas.width / 2);
          const dy = (y - canvas.height / 2) / (canvas.height / 2);
          const fade = Math.max(0, 1 - Math.hypot(dx, dy * 0.9));
          if (fade <= 0) continue;
          ctx.globalAlpha = Math.min(1, fade * 1.6);
          ctx.fillStyle = palette.dot;
          ctx.beginPath();
          ctx.arc(x, y, 3, 0, Math.PI * 2);
          ctx.fill();
        }
      }
    }
    const map = new THREE.CanvasTexture(canvas);
    map.colorSpace = THREE.SRGBColorSpace;
    map.anisotropy = 8;
    return map;
  }, [palette.dot]);
  useEffect(() => () => texture.dispose(), [texture]);
  return (
    <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, 0.002, -0.5]}>
      <planeGeometry args={[50, 25]} />
      <meshBasicMaterial map={texture} transparent depthWrite={false} />
    </mesh>
  );
}

/** Andon tower: green / amber / red, lit from the station's health; amber flashes for a recorded decision. */
function StackLight({
  position,
  health,
  attention,
  animate,
}: {
  position: [number, number, number];
  health: HealthState;
  attention: boolean;
  animate: boolean;
}) {
  const tone = healthTone(health);
  const lit: Record<string, boolean> = {
    ok: tone === "ok" && !attention,
    warn: tone === "warn" || attention,
    crit: tone === "crit",
  };
  const amber = useRef<THREE.MeshStandardMaterial>(null);
  useFrame(({ clock }) => {
    if (!attention || !amber.current) return;
    const on = !animate || Math.sin(clock.getElapsedTime() * 6) > -0.2;
    amber.current.emissiveIntensity = on ? 2.4 : 0.15;
  });
  const segments: Array<[keyof typeof lit, string]> = [
    ["ok", TONE_COLOR.ok],
    ["warn", TONE_COLOR.warn],
    ["crit", TONE_COLOR.crit],
  ];
  return (
    <group position={position}>
      <mesh position={[0, 0.12, 0]}>
        <cylinderGeometry args={[0.022, 0.022, 0.24, 8]} />
        <Matte color="#5d6870" />
      </mesh>
      {segments.map(([key, color], i) => (
        <mesh key={key} position={[0, 0.31 + i * 0.13, 0]}>
          <cylinderGeometry args={[0.065, 0.065, 0.115, 20]} />
          <meshStandardMaterial
            ref={key === "warn" ? amber : undefined}
            color={color}
            emissive={color}
            emissiveIntensity={lit[key] ? 2.4 : 0.04}
            transparent
            opacity={lit[key] ? 1 : 0.35}
            toneMapped={!lit[key]}
          />
        </mesh>
      ))}
      <mesh position={[0, 0.69, 0]}>
        <cylinderGeometry args={[0.07, 0.07, 0.03, 20]} />
        <Matte color="#2b3338" />
      </mesh>
      {tone === "crit" && <pointLight color={TONE_COLOR.crit} intensity={2} distance={2.6} position={[0, 0.6, 0.2]} />}
    </group>
  );
}

function Selectable({
  id,
  onSelect,
  children,
}: {
  id: CellStationId;
  onSelect: (station: CellStationId) => void;
  children: ReactNode;
}) {
  const { gl } = useThree();
  useEffect(
    () => () => {
      gl.domElement.style.cursor = "";
    },
    [gl],
  );
  return (
    <group
      onClick={(event: ThreeEvent<MouseEvent>) => {
        event.stopPropagation();
        onSelect(id);
      }}
      onPointerOver={() => {
        gl.domElement.style.cursor = "pointer";
      }}
      onPointerOut={() => {
        gl.domElement.style.cursor = "";
      }}
    >
      {children}
    </group>
  );
}

/** The station card: a rounded panel behind the line, outlined when selected, label printed bottom-left. */
function StationCard({
  id,
  palette,
  selected,
}: {
  id: CellStationId;
  palette: ScenePalette;
  selected: boolean;
}) {
  const width = STATION_WIDTH[id];
  const x = STATION_X[id];
  return (
    <group>
      <Block position={[x, CARD_TOP / 2, CARD_Z]} size={[width, CARD_TOP, CARD_DEPTH]} color={palette.card} radius={0.07} />
      <RoundedBox
        args={[width + (selected ? 0.12 : 0.05), CARD_TOP + (selected ? 0.12 : 0.05), CARD_DEPTH * 0.6]}
        radius={0.09}
        smoothness={4}
        position={[x, CARD_TOP / 2, CARD_Z - 0.05]}
      >
        <meshBasicMaterial color={selected ? palette.selection : palette.cardEdge} />
      </RoundedBox>
      <Label
        text={STATION_INFO[id].code}
        color={palette.label}
        height={0.26}
        anchor="left"
        position={[x - width / 2 + 0.3, BELT_TOP + 0.42, CARD_Z + CARD_DEPTH / 2 + 0.005]}
      />
    </group>
  );
}

/** Rotating amber beacon raised on the gantry when maintenance is requested. */
function Beacon({ position, animate }: { position: [number, number, number]; animate: boolean }) {
  const reflector = useRef<THREE.Group>(null);
  const light = useRef<THREE.PointLight>(null);
  useFrame(({ clock }, delta) => {
    if (!animate) return;
    if (reflector.current) reflector.current.rotation.y += delta * 5;
    if (light.current) light.current.intensity = 1.4 + Math.sin(clock.getElapsedTime() * 10) * 0.9;
  });
  return (
    <group position={position}>
      <mesh position={[0, 0.06, 0]}>
        <cylinderGeometry args={[0.15, 0.17, 0.12, 24]} />
        <Matte color="#2b3338" />
      </mesh>
      <mesh position={[0, 0.24, 0]}>
        <cylinderGeometry args={[0.12, 0.12, 0.24, 24]} />
        <meshStandardMaterial
          color={TONE_COLOR.warn}
          emissive={TONE_COLOR.warn}
          emissiveIntensity={1.8}
          transparent
          opacity={0.88}
          toneMapped={false}
        />
      </mesh>
      <group ref={reflector} position={[0, 0.24, 0]}>
        <mesh position={[0.07, 0, 0]}>
          <boxGeometry args={[0.02, 0.18, 0.15]} />
          <meshBasicMaterial color="#fff3d6" toneMapped={false} />
        </mesh>
      </group>
      <pointLight ref={light} color={TONE_COLOR.warn} intensity={1.4} distance={4.5} position={[0, 0.4, 0]} />
    </group>
  );
}

function heatColor(heat: number, base: string): THREE.Color {
  const steel = new THREE.Color(base);
  const amber = new THREE.Color(TONE_COLOR.warn);
  const red = new THREE.Color(TONE_COLOR.crit);
  return heat < 0.5 ? steel.lerp(amber, heat * 2) : amber.lerp(red, (heat - 0.5) * 2);
}

/** One numbered spindle hanging from the gantry: head, collar, fluted tool, tip. */
function Spindle({
  index,
  palette,
  heat,
  speed,
  running,
  animate,
}: {
  index: number;
  palette: ScenePalette;
  /** 0..1; only Spindle 3 is instrumented, the others stay at 0. */
  heat: number;
  /** Turns per second at full speed. */
  speed: number;
  running: boolean;
  animate: boolean;
}) {
  const tool = useRef<THREE.Group>(null);
  const feed = useRef<THREE.Group>(null);
  const halo = useRef<THREE.Mesh>(null);
  const spin = useRef(running ? speed : 0);
  const plunge = useRef(0);
  useFrame(({ clock }, delta) => {
    if (!animate) return;
    spin.current = THREE.MathUtils.damp(spin.current, running ? speed : 0, running ? 2 : 0.9, delta);
    if (tool.current) tool.current.rotation.y += delta * spin.current * Math.PI * 2;
    const cycle = running ? Math.max(0, Math.sin(clock.getElapsedTime() * 1.3 + index * 2.1)) * 0.16 : 0;
    plunge.current = THREE.MathUtils.damp(plunge.current, cycle, 4, delta);
    if (feed.current) feed.current.position.y = -plunge.current;
    if (halo.current) {
      const material = halo.current.material as THREE.MeshBasicMaterial;
      const pulse = running ? 0.75 + 0.25 * Math.sin(clock.getElapsedTime() * 4) : 1;
      material.opacity = Math.min(0.85, heat * 0.9) * pulse;
    }
  });
  const x = SPINDLE_X[index];
  const head = heat > 0 ? heatColor(heat * 0.75, palette.head) : new THREE.Color(palette.head);
  const toolColor = heat > 0 ? heatColor(heat, palette.tool) : new THREE.Color(palette.tool);
  const labelColor = heat > 0.5 ? "#ffffff" : palette.headLabel;
  return (
    <group position={[x, 0, 0]}>
      <mesh position={[0, BEAM_Y - 0.3, 0]}>
        <boxGeometry args={[0.36, 0.3, 0.36]} />
        <Matte color={palette.ink} />
      </mesh>
      <group ref={feed}>
        <RoundedBox args={[0.78, 0.95, 0.6]} radius={0.06} smoothness={4} position={[0, 3.08, 0]} castShadow receiveShadow>
          <Matte color={`#${head.getHexString()}`} emissive={heat > 0.5 ? TONE_COLOR.crit : "#000"} emissiveIntensity={heat > 0.5 ? (heat - 0.5) * 0.5 : 0} />
        </RoundedBox>
        <Label text={String(index + 1)} color={labelColor} height={0.42} tracking={0} position={[0, 3.12, 0.305]} />
        <mesh position={[0, 2.55, 0]}>
          <cylinderGeometry args={[0.16, 0.13, 0.12, 24]} />
          <Matte color={palette.ink} />
        </mesh>
        <group ref={tool} position={[0, 0, 0]}>
          <mesh position={[0, 2.12, 0]} castShadow>
            <cylinderGeometry args={[0.085, 0.085, 0.76, 20]} />
            <meshStandardMaterial
              color={toolColor}
              emissive={toolColor}
              emissiveIntensity={heat * 1.6}
              metalness={0.55}
              roughness={0.32}
              toneMapped={heat < 0.6}
            />
          </mesh>
          {[0, Math.PI].map((angle) => (
            <mesh key={angle} position={[Math.cos(angle) * 0.078, 2.0, Math.sin(angle) * 0.078]} rotation={[0, -angle, 0.22]}>
              <boxGeometry args={[0.022, 0.5, 0.022]} />
              <Matte color={palette.ink} />
            </mesh>
          ))}
          <mesh position={[0, 1.6, 0]} rotation={[Math.PI, 0, 0]} castShadow>
            <coneGeometry args={[0.085, 0.3, 20]} />
            <meshStandardMaterial
              color={toolColor}
              emissive={toolColor}
              emissiveIntensity={heat * 1.6}
              metalness={0.55}
              roughness={0.32}
              toneMapped={heat < 0.6}
            />
          </mesh>
        </group>
        {heat > 0 && (
          <>
            <mesh ref={halo} position={[0, 2.5, 0]} rotation={[Math.PI / 2, 0, 0]}>
              <torusGeometry args={[0.26, 0.022, 12, 48]} />
              <meshBasicMaterial color={heat > 0.5 ? TONE_COLOR.crit : TONE_COLOR.warn} transparent opacity={0} toneMapped={false} />
            </mesh>
            <pointLight color={TONE_COLOR.crit} intensity={heat * heat * 8} distance={2.8} position={[0, 2.0, 0.5]} />
          </>
        )}
      </group>
    </group>
  );
}

function Mill({
  palette,
  heat,
  rpm,
  animate,
  running,
  maintenance,
}: {
  palette: ScenePalette;
  heat: number;
  rpm: number;
  animate: boolean;
  /** False while the cell is held: the spindles wind down instead of stopping dead. */
  running: boolean;
  maintenance: boolean;
}) {
  const width = STATION_WIDTH.mill + 0.2;
  const x = STATION_X.mill;
  const hotSpeed = 0.6 + rpm / 1500;
  return (
    <group>
      {/* Gantry beam across the top of the card, with end plates that tie it to the card. */}
      <Block position={[x, BEAM_Y, -0.35]} size={[width, 0.3, 1.2]} color={palette.ink} radius={0.06} roughness={0.5} />
      {[-1, 1].map((side) => (
        <Block
          key={side}
          position={[x + side * (width / 2 - 0.12), BEAM_Y - 0.25, CARD_Z + 0.2]}
          size={[0.24, 0.5, 0.3]}
          color={palette.ink}
          radius={0.04}
        />
      ))}
      {SPINDLE_X.map((_, i) => (
        <Spindle
          key={i}
          index={i}
          palette={palette}
          heat={i === HOT_SPINDLE ? heat : 0}
          speed={i === HOT_SPINDLE ? hotSpeed : 1.8}
          running={running}
          animate={animate}
        />
      ))}
      {/* Coolant nozzle aimed at Spindle 3's cut. */}
      <mesh position={[HOT_X - 0.27, 2.2, 0.22]} rotation={[0, 0, 0.29]}>
        <cylinderGeometry args={[0.025, 0.02, 0.8, 8]} />
        <Matte color="#4c8fb8" />
      </mesh>
      {/* Coolant tank at the head of the line. */}
      <Block position={[-9.75, 0.55, -0.35]} size={[0.8, 1.1, 1.0]} color={palette.card} radius={0.06} />
      <mesh position={[-9.75, 0.72, 0.16]}>
        <planeGeometry args={[0.5, 0.08]} />
        <meshBasicMaterial color="#4c8fb8" />
      </mesh>
      {maintenance && <Beacon position={[x - width / 2 + 0.6, BEAM_Y + 0.15, -0.35]} animate={animate} />}
    </group>
  );
}

/** Glass thermometer for Spindle 3: the fill is the live reading, the dashed line is the approved limit. */
function LimitGauge({
  palette,
  temperature,
  limit,
  overLimit,
}: {
  palette: ScenePalette;
  temperature?: number;
  limit?: number;
  overLimit: boolean;
}) {
  const fillRef = useRef<THREE.Mesh>(null);
  const level = useRef(0);
  const height = TUBE_TOP - TUBE_BOTTOM;
  const target =
    temperature !== undefined && limit ? Math.min(1, Math.max(0.02, temperature / (limit * GAUGE_SPAN))) : 0;
  useFrame((_, delta) => {
    level.current = THREE.MathUtils.damp(level.current, target, 3, delta);
    const mesh = fillRef.current;
    if (!mesh) return;
    const h = Math.max(0.001, level.current * (height - 0.08));
    mesh.scale.y = h;
    mesh.position.y = TUBE_BOTTOM + 0.04 + h / 2;
  });
  const near = target > 0.8 / GAUGE_SPAN;
  const fill = overLimit ? TONE_COLOR.crit : near ? TONE_COLOR.warn : palette.tool;
  const limitY = TUBE_BOTTOM + height / GAUGE_SPAN;
  const z = -0.62;
  return (
    <group position={[LIMIT_X, 0, z]}>
      <mesh position={[0, (TUBE_BOTTOM + TUBE_TOP) / 2, 0]}>
        <capsuleGeometry args={[0.19, height - 0.38, 8, 24]} />
        <meshPhysicalMaterial
          color={palette.glass}
          transparent
          opacity={0.32}
          roughness={0.08}
          metalness={0}
          clearcoat={1}
          depthWrite={false}
        />
      </mesh>
      <mesh ref={fillRef} position={[0, TUBE_BOTTOM + 0.04, 0]}>
        <cylinderGeometry args={[0.13, 0.13, 1, 24]} />
        <meshStandardMaterial
          color={fill}
          emissive={fill}
          emissiveIntensity={overLimit ? 1.2 : near ? 0.5 : 0.08}
          roughness={0.4}
          toneMapped={!overLimit}
        />
      </mesh>
      <mesh position={[0, TUBE_BOTTOM - 0.06, 0]} castShadow>
        <cylinderGeometry args={[0.26, 0.28, 0.14, 28]} />
        <Matte color={palette.ink} />
      </mesh>
      <mesh position={[0, (TUBE_BOTTOM - 0.13) / 2, 0]}>
        <boxGeometry args={[0.08, TUBE_BOTTOM - 0.13, 0.08]} />
        <Matte color={palette.ink} />
      </mesh>
      {limit !== undefined && (
        <group position={[0, limitY, 0]}>
          {Array.from({ length: 5 }, (_, i) => (
            <mesh key={i} position={[-0.32 - i * 0.12, 0, 0.05]}>
              <boxGeometry args={[0.07, 0.025, 0.025]} />
              <meshBasicMaterial color={palette.ink} />
            </mesh>
          ))}
          <mesh rotation={[Math.PI / 2, 0, 0]}>
            <torusGeometry args={[0.2, 0.012, 8, 40]} />
            <meshBasicMaterial color={palette.ink} />
          </mesh>
        </group>
      )}
      <Label text="LIMIT" color={palette.label} height={0.22} position={[0, TUBE_TOP + 0.22, 0]} />
      {limit !== undefined && (
        <Label
          text={`${formatNumber(limit, 0)} °C`}
          color={palette.label}
          height={0.2}
          tracking={0.04}
          weight={500}
          anchor="left"
          position={[0.3, limitY, 0.05]}
        />
      )}
    </group>
  );
}

function VisionCamera({
  palette,
  animate,
  requested,
  scanning,
}: {
  palette: ScenePalette;
  animate: boolean;
  requested: boolean;
  /** Held parts are being inspected: the light sweeps fast and bright. */
  scanning: boolean;
}) {
  const cone = useRef<THREE.Mesh>(null);
  const spot = useRef<THREE.Mesh>(null);
  useFrame(({ clock }) => {
    const t = clock.getElapsedTime();
    const base = scanning ? 0.26 : requested ? 0.22 : 0.16;
    const pulse = animate && scanning ? 0.08 * Math.sin(t * 7) : 0;
    if (cone.current) (cone.current.material as THREE.MeshBasicMaterial).opacity = base + pulse;
    if (spot.current) {
      spot.current.position.x = animate ? Math.sin(t * (scanning ? 3.2 : 1.4)) * 0.55 : 0;
      (spot.current.material as THREE.MeshBasicMaterial).opacity = scanning ? 0.7 : requested ? 0.55 : 0.32;
    }
  });
  const x = STATION_X.inspect;
  const camY = CARD_TOP + 0.35;
  const beam = requested || scanning ? TONE_COLOR.warn : palette.cone;
  const coneHeight = camY - 0.22 - BELT_TOP;
  return (
    <group position={[x, 0, 0]}>
      {/* Boom from the top of the card to the camera over the belt. */}
      <Block position={[0, camY + 0.28, -0.43]} size={[0.16, 0.12, 0.95]} color={palette.ink} radius={0.03} />
      <Block
        position={[0, (CARD_TOP + camY + 0.34) / 2, CARD_Z]}
        size={[0.16, camY + 0.34 - CARD_TOP, 0.12]}
        color={palette.ink}
        radius={0.03}
      />
      <Block position={[0, camY, 0]} size={[0.9, 0.44, 0.52]} color={palette.ink} radius={0.08} roughness={0.45} />
      <mesh position={[0, camY, 0.262]}>
        <circleGeometry args={[0.12, 32]} />
        <meshBasicMaterial color="#ffffff" />
      </mesh>
      <mesh position={[0, camY, 0.264]}>
        <circleGeometry args={[0.055, 24]} />
        <meshBasicMaterial color={requested || scanning ? TONE_COLOR.warn : palette.ink} toneMapped={false} />
      </mesh>
      <mesh position={[0, camY - 0.23, 0]}>
        <cylinderGeometry args={[0.1, 0.12, 0.04, 24]} />
        <meshStandardMaterial color="#111" metalness={0.8} roughness={0.15} />
      </mesh>
      <mesh ref={cone} position={[0, BELT_TOP + coneHeight / 2, 0]}>
        <coneGeometry args={[0.82, coneHeight, 48, 1, true]} />
        <meshBasicMaterial color={beam} transparent opacity={0.16} side={THREE.DoubleSide} depthWrite={false} toneMapped={false} />
      </mesh>
      <mesh ref={spot} position={[0, BELT_TOP + 0.012, 0]} rotation={[-Math.PI / 2, 0, 0]}>
        <planeGeometry args={[0.06, BELT_DEPTH * 0.9]} />
        <meshBasicMaterial color={beam} transparent opacity={0.32} depthWrite={false} toneMapped={false} />
      </mesh>
      {scanning && <pointLight color={TONE_COLOR.warn} intensity={2.4} distance={3.2} position={[0, camY - 0.6, 0.2]} />}
    </group>
  );
}

function PackStation({ palette, hold }: { palette: ScenePalette; hold: boolean }) {
  const arm = useRef<THREE.Group>(null);
  useFrame((_, delta) => {
    if (!arm.current) return;
    const target = hold ? 0 : Math.PI / 2.2;
    arm.current.rotation.x = THREE.MathUtils.damp(arm.current.rotation.x, target, 6, delta);
  });
  const x = STATION_X.pack;
  const shelfY = 2.05;
  const carton: [number, number, number] = [1.05, 0.78, 0.8];
  const top = shelfY + 0.04 + carton[1];
  return (
    <group>
      {/* Carton on its shelf: the pack station's case erector. */}
      <Block position={[x, shelfY, -0.42]} size={[1.6, 0.08, 0.86]} color={palette.ink} radius={0.02} />
      <Block position={[x, shelfY + 0.04 + carton[1] / 2, -0.42]} size={carton} color={palette.carton} radius={0.03} />
      <mesh position={[x, top - 0.2, -0.42 + carton[2] / 2 + 0.003]}>
        <planeGeometry args={[carton[0] - 0.04, 0.03]} />
        <meshBasicMaterial color={palette.ink} />
      </mesh>
      <mesh position={[x, top - 0.1, -0.42 + carton[2] / 2 + 0.003]}>
        <planeGeometry args={[0.03, 0.2]} />
        <meshBasicMaterial color={palette.ink} />
      </mesh>
      <mesh position={[x, top + 0.003, -0.42]} rotation={[-Math.PI / 2, 0, 0]}>
        <planeGeometry args={[0.05, carton[2] - 0.04]} />
        <meshBasicMaterial color={palette.ink} transparent opacity={0.55} />
      </mesh>
      {/* Hold gate on the PACK infeed: lowered only when a hold is recorded. */}
      <group position={[GATE_X, 0, BELT_DEPTH / 2 + 0.18]}>
        <Block position={[0, 0.75, 0]} size={[0.14, 1.5, 0.14]} color={palette.ink} radius={0.03} />
        <group ref={arm} position={[0, 1.42, 0]} rotation={[Math.PI / 2.2, 0, 0]}>
          {Array.from({ length: 5 }, (_, i) => (
            <mesh key={i} position={[0, 0, -0.12 - i * 0.24]} castShadow>
              <boxGeometry args={[0.08, 0.08, 0.24]} />
              <meshStandardMaterial
                color={i % 2 === 0 ? TONE_COLOR.warn : "#1d1f20"}
                emissive={i % 2 === 0 && hold ? TONE_COLOR.warn : "#000"}
                emissiveIntensity={hold ? 0.5 : 0}
              />
            </mesh>
          ))}
        </group>
      </group>
      {hold && (
        <mesh position={[GATE_X - 1.3, BELT_TOP + 0.012, 0]} rotation={[-Math.PI / 2, 0, 0]}>
          <planeGeometry args={[2.6, BELT_DEPTH]} />
          <meshBasicMaterial color={TONE_COLOR.warn} transparent opacity={0.18} depthWrite={false} />
        </mesh>
      )}
      {/* Line drive motor at the end of the belt (pack line sensors). */}
      <Block position={[9.35, 0.6, 0.55]} size={[0.5, 0.5, 0.42]} color={palette.ink} radius={0.05} />
      <mesh position={[9.35, 0.6, 0.3]} rotation={[Math.PI / 2, 0, 0]}>
        <cylinderGeometry args={[0.15, 0.15, 0.1, 20]} />
        <Matte color={palette.tool} />
      </mesh>
    </group>
  );
}

const ROLLER_PITCH = 1.43;
const ROLLER_R = 0.11;

function Conveyor({
  palette,
  speed,
  animate,
  stopped,
  queued,
}: {
  palette: ScenePalette;
  speed: number;
  animate: boolean;
  /** Line held: the belt coasts to a stop. */
  stopped: boolean;
  /** Batch held at PACK: parts bunch up behind the lowered gate. */
  queued: boolean;
}) {
  const parts = useRef<Array<THREE.Group | null>>([]);
  const rollers = useRef<Array<THREE.Mesh | null>>([]);
  const belt = useRef(stopped ? 0 : 1);
  const span = BELT_END - BELT_START;
  const count = Math.floor(span / PART_PITCH);
  useFrame((_, delta) => {
    if (!animate) return;
    belt.current = THREE.MathUtils.damp(belt.current, stopped ? 0 : 1, stopped ? 1.4 : 2.5, delta);
    const step = delta * Math.max(0.6, speed) * 0.22 * belt.current;
    for (const roller of rollers.current) if (roller) roller.rotation.y -= step / ROLLER_R;
    const live = parts.current
      .filter((part): part is THREE.Group => part !== null)
      .sort((a, b) => b.position.x - a.position.x);
    let limit = GATE_X - 0.5;
    for (const part of live) {
      if (queued && part.position.x <= GATE_X) {
        part.position.x = Math.min(part.position.x + step, Math.max(part.position.x, limit));
        limit = part.position.x - PART_PITCH * 0.62;
        continue;
      }
      part.position.x += step;
      if (part.position.x > BELT_END - 0.3) part.position.x -= span - 0.3;
    }
  });
  const length = span;
  const center = (BELT_START + BELT_END) / 2;
  const rollerCount = Math.floor(length / ROLLER_PITCH) + 1;
  return (
    <group>
      <RoundedBox
        args={[length, 0.16, BELT_DEPTH]}
        radius={0.075}
        smoothness={4}
        position={[center, BELT_TOP - 0.08, 0]}
        castShadow
        receiveShadow
      >
        <Matte color={palette.ink} roughness={0.75} />
      </RoundedBox>
      {Array.from({ length: rollerCount }, (_, i) => (
        <group key={i} position={[BELT_START + 0.35 + i * ROLLER_PITCH, BELT_TOP - 0.16 - ROLLER_R, 0]}>
          <mesh
            ref={(el) => {
              rollers.current[i] = el;
            }}
            rotation={[Math.PI / 2, 0, 0]}
          >
            <cylinderGeometry args={[ROLLER_R, ROLLER_R, BELT_DEPTH * 0.86, 20]} />
            <Matte color={palette.roller} />
          </mesh>
          {[-1, 1].map((side) => (
            <mesh key={side} position={[0, 0, side * (BELT_DEPTH * 0.43 + 0.006)]}>
              <torusGeometry args={[ROLLER_R, 0.014, 8, 24]} />
              <meshBasicMaterial color={palette.ink} />
            </mesh>
          ))}
        </group>
      ))}
      {Array.from({ length: Math.ceil(length / 3) + 1 }, (_, i) => (
        <group key={i} position={[BELT_START + 0.4 + Math.min(i * 3, length - 0.8), 0, 0]}>
          {[-1, 1].map((side) => (
            <mesh key={side} position={[0, (BELT_TOP - 0.16) / 2, side * 0.46]}>
              <boxGeometry args={[0.07, BELT_TOP - 0.16, 0.07]} />
              <Matte color={palette.ink} />
            </mesh>
          ))}
        </group>
      ))}
      {Array.from({ length: count }, (_, i) => (
        <group
          key={i}
          ref={(el) => {
            parts.current[i] = el;
          }}
          position={[BELT_START + 0.3 + i * PART_PITCH, BELT_TOP + 0.13, 0]}
        >
          <RoundedBox args={[0.62, 0.26, 0.52]} radius={0.07} smoothness={4} castShadow receiveShadow>
            <Matte color={palette.part} roughness={0.45} />
          </RoundedBox>
          <mesh position={[0, 0.132, 0]} rotation={[-Math.PI / 2, 0, 0]}>
            <ringGeometry args={[0.06, 0.1, 24]} />
            <meshBasicMaterial color={palette.ink} transparent opacity={0.35} />
          </mesh>
        </group>
      ))}
    </group>
  );
}

function Utilities({ palette }: { palette: ScenePalette }) {
  return (
    <group>
      {/* Shop air compressor feeding the cell */}
      <group position={[-9.6, 0, -2.3]}>
        <Block position={[0, 0.55, 0]} size={[1.2, 1.1, 0.85]} color={palette.card} radius={0.07} />
        <mesh position={[0.9, 0.8, 0]} rotation={[0, 0, 0]} castShadow>
          <capsuleGeometry args={[0.3, 1.0, 8, 24]} />
          <Matte color={palette.head} />
        </mesh>
        <mesh position={[0, 1.2, 0]}>
          <sphereGeometry args={[0.07, 12, 12]} />
          <Matte color={palette.ink} />
        </mesh>
      </group>
      {/* Ambient climate sensor on a building column */}
      <group position={[9.7, 0, -2.1]}>
        <Block position={[0, 2, -0.15]} size={[0.5, 4, 0.5]} color={palette.card} radius={0.05} />
        <Block position={[0, 2.75, 0.14]} size={[0.26, 0.34, 0.1]} color={palette.ink} radius={0.03} />
      </group>
    </group>
  );
}

/**
 * `hero` and `label` draw the reading beside the machine; `marker` draws a
 * numbered pin keyed to the sensor legend, so close-set readings never collide.
 */
type CalloutStyle = { kind: "hero" } | { kind: "marker"; n: number };

function SensorCallout({ sensor, style, limit }: { sensor: CellSensorView; style: CalloutStyle; limit?: number }) {
  const anchor = SENSOR_ANCHORS[sensor.name];
  if (!anchor) return null;
  const value = sensor.value === undefined ? "—" : formatNumber(sensor.value, sensor.decimals);
  const hero = style.kind === "hero";
  const fill =
    hero && limit && sensor.value !== undefined ? Math.min(1, Math.max(0, sensor.value / (limit * GAUGE_SPAN))) : 0;
  if (style.kind === "marker") {
    return (
      <Html position={anchor.at} center zIndexRange={[30, 10]} style={{ pointerEvents: "none" }}>
        <span className={`cell-marker tnum${sensor.overLimit ? " is-over" : ""}`} aria-hidden="true">
          {style.n}
        </span>
      </Html>
    );
  }
  return (
    <Html position={anchor.at} zIndexRange={[30, 10]} style={{ pointerEvents: "none" }}>
      <div
        className={`cell-pin cell-pin--${anchor.side}${hero ? " cell-pin--hero" : ""}${sensor.overLimit ? " is-over" : ""}`}
      >
        <i aria-hidden="true" />
        <div className="cell-pin__label">
          <span>{sensor.label}</span>
          <b className="tnum">
            {value}
            <small> {sensor.unit}</small>
          </b>
          {hero && limit !== undefined && (
            <em className="cell-pin__gauge" style={{ ["--fill" as string]: fill, ["--limit" as string]: 1 / GAUGE_SPAN }} />
          )}
        </div>
      </div>
    </Html>
  );
}

const RIPPLE_SECONDS = 1.6;

/** Expanding floor ring under the station a decision was recorded for. */
function Ripple({ cue, delay, animate }: { cue: CellCue; delay: number; animate: boolean }) {
  const ring = useRef<THREE.Mesh>(null);
  useFrame(() => {
    const mesh = ring.current;
    if (!mesh) return;
    const t = ((Date.now() - cue.at) / 1000 - delay) / RIPPLE_SECONDS;
    mesh.visible = animate && t >= 0 && t < 1;
    if (!mesh.visible) return;
    const scale = 1 + t * 3;
    mesh.scale.set(scale, scale, 1);
    (mesh.material as THREE.MeshBasicMaterial).opacity = 0.75 * (1 - t);
  });
  return (
    <mesh ref={ring} position={[STATION_X[cue.station], 0.03, 0.4]} rotation={[-Math.PI / 2, 0, 0]} visible={false}>
      <ringGeometry args={[0.9, 1.04, 64]} />
      <meshBasicMaterial color={TONE_COLOR.warn} transparent opacity={0} depthWrite={false} toneMapped={false} />
    </mesh>
  );
}

const HERO_SENSOR = "cnc_1_spindle_temp";

/** Sensors listed in the legend for a view, in pin-number order. The whole cell lists shop utilities. */
function legendFor(sensors: CellSensorView[], view: CellView): CellSensorView[] {
  const group: CellSensorGroup = view === "cell" ? "utilities" : view;
  return sensors.filter((s) => s.group === group && s.name !== HERO_SENSOR);
}

function calloutStyle(sensor: CellSensorView, view: CellView, legend: CellSensorView[]): CalloutStyle | null {
  if (sensor.name === HERO_SENSOR && view === "mill") return { kind: "hero" };
  const n = legend.indexOf(sensor);
  return n < 0 ? null : { kind: "marker", n: n + 1 };
}

function SensorLegend({ sensors, view }: { sensors: CellSensorView[]; view: CellView }) {
  const legend = legendFor(sensors, view);
  if (legend.length === 0) return null;
  const code = view === "cell" ? "CELL" : STATION_INFO[view].code;
  const name = view === "cell" ? "Shop utilities" : `${STATION_INFO[view].name} sensors`;
  return (
    <section className="cell-legend" aria-label={name}>
      <header>
        <span>{code}</span>
        {view === "cell" ? name : "Live sensors"}
      </header>
      <ol>
        {legend.map((sensor, i) => (
          <li key={sensor.name} className={sensor.overLimit ? "is-over" : undefined}>
            <span className="cell-marker tnum" aria-hidden="true">
              {i + 1}
            </span>
            <span className="cell-legend__label">{sensor.label}</span>
            <b className="tnum">
              {sensor.value === undefined ? "—" : formatNumber(sensor.value, sensor.decimals)}
              <small> {sensor.unit}</small>
            </b>
          </li>
        ))}
      </ol>
    </section>
  );
}

/** MILL's tag sits over spindles 1-2 so Spindle 3's temperature pin has the space above it. */
const TAG_AT: Record<CellStationId, [number, number, number]> = {
  mill: [SPINDLE_X[0] + 0.6, BEAM_Y + 0.75, CARD_Z],
  inspect: [STATION_X.inspect, CARD_TOP + 1.25, CARD_Z],
  pack: [STATION_X.pack, CARD_TOP + 0.6, CARD_Z],
};

function StationTag({
  id,
  view,
  decision,
}: {
  id: CellStationId;
  view: StationView;
  decision?: string;
}) {
  const info = STATION_INFO[id];
  return (
    <Html position={TAG_AT[id]} center zIndexRange={[25, 5]} style={{ pointerEvents: "none" }}>
      <div className={`cell-tag tone-${healthTone(view.health)}`}>
        <span className="cell-tag__code">{info.code}</span>
        <strong>{info.name}</strong>
        <span className="cell-tag__readout tnum">{view.readout}</span>
        {decision && <span className="cell-tag__decision">{decision}</span>}
      </div>
    </Html>
  );
}

function framingFor(view: CellView, camera: THREE.PerspectiveCamera, aspect: number) {
  const f = FRAMING[view];
  const vfov = THREE.MathUtils.degToRad(camera.fov);
  const hfov = 2 * Math.atan(Math.tan(vfov / 2) * aspect);
  const distance = Math.max(f.height / Math.tan(vfov / 2), f.width / Math.tan(hfov / 2)) * 0.92;
  const target = new THREE.Vector3(...f.target);
  const position = target.clone().addScaledVector(new THREE.Vector3(...f.direction).normalize(), distance);
  return { target, position };
}

/** Flies the camera to the selected framing; user orbiting cancels the flight. */
function CameraRig({ view, instant }: { view: CellView; instant: boolean }) {
  const { camera, size } = useThree();
  const controls = useThree((state) => state.controls) as ElementRef<typeof OrbitControls> | null;
  const flight = useRef<{ target: THREE.Vector3; position: THREE.Vector3 } | null>(null);
  const first = useRef(true);

  useEffect(() => {
    if (!(camera instanceof THREE.PerspectiveCamera)) return;
    const goal = framingFor(view, camera, size.width / Math.max(1, size.height));
    if (instant || first.current) {
      camera.position.copy(goal.position);
      controls?.target.copy(goal.target);
      camera.lookAt(goal.target);
      controls?.update();
      flight.current = null;
      first.current = !controls;
      return;
    }
    flight.current = goal;
  }, [view, camera, controls, size.width, size.height, instant]);

  useEffect(() => {
    if (!controls) return;
    const cancel = () => {
      flight.current = null;
    };
    controls.addEventListener("start", cancel);
    return () => controls.removeEventListener("start", cancel);
  }, [controls]);

  useFrame((_, delta) => {
    const goal = flight.current;
    if (!goal || !controls) return;
    const k = 1 - Math.exp(-delta * 3.2);
    camera.position.lerp(goal.position, k);
    controls.target.lerp(goal.target, k);
    controls.update();
    if (camera.position.distanceTo(goal.position) < 0.02 && controls.target.distanceTo(goal.target) < 0.02) {
      flight.current = null;
    }
  });
  return null;
}

const STACK_LIGHT_AT: Record<CellStationId, [number, number, number]> = {
  mill: [STATION_X.mill + STATION_WIDTH.mill / 2 - 0.35, BEAM_Y + 0.15, -0.7],
  inspect: [STATION_X.inspect + STATION_WIDTH.inspect / 2 - 0.3, CARD_TOP, CARD_Z],
  pack: [STATION_X.pack + STATION_WIDTH.pack / 2 - 0.3, CARD_TOP, CARD_Z],
};

function CellScene(props: MachiningCellProps & { view: CellView; theme: ResolvedTheme; reducedMotion: boolean }) {
  const {
    stations,
    sensors,
    selected,
    onSelect,
    decisions,
    batchHeld,
    spindleHeat,
    spindleLimit,
    spindleRpm = 0,
    lineSpeed = 0,
    animate,
    view,
    theme,
    reducedMotion,
    stop,
    cue,
  } = props;
  const palette = PALETTE[theme];
  const held = stop?.phase === "stopped";
  const hero = sensors.find((s) => s.name === HERO_SENSOR);
  const attention: Record<CellStationId, boolean> = {
    mill: held || !!decisions.request_maintenance,
    inspect: !!decisions.send_to_inspect,
    pack: held || batchHeld,
  };
  const decisionText = (station: CellStationId) =>
    RESPONSE_STEPS.filter((step) => step.station === station && decisions[step.action])
      .map((step) => decisionLabel(step, batchHeld))
      .join(" · ") || undefined;

  return (
    <>
      <ambientLight intensity={palette.ambient} />
      <hemisphereLight args={[palette.fill, palette.card, 0.35]} />
      <directionalLight
        castShadow
        intensity={palette.key}
        position={[5, 12, 10]}
        shadow-mapSize={[2048, 2048]}
        shadow-camera-left={-13}
        shadow-camera-right={13}
        shadow-camera-top={8}
        shadow-camera-bottom={-8}
        shadow-bias={-0.0004}
        shadow-normalBias={0.02}
      />
      <Environment resolution={128} frames={1}>
        <Lightformer form="rect" intensity={2.2} position={[0, 6, 6]} scale={[16, 2, 1]} />
        <Lightformer form="rect" intensity={1.0} position={[-9, 3, 0]} rotation-y={Math.PI / 2} scale={[8, 2, 1]} />
        <Lightformer form="rect" intensity={0.8} position={[9, 3, 0]} rotation-y={-Math.PI / 2} scale={[8, 2, 1]} />
      </Environment>
      <DotFloor palette={palette} />
      <ContactShadows position={[0, 0.004, -0.2]} scale={[26, 9]} blur={2.4} far={4.5} resolution={512} opacity={palette.shadow} />
      <Conveyor palette={palette} speed={lineSpeed} animate={animate} stopped={held} queued={batchHeld} />
      <Utilities palette={palette} />
      <LimitGauge palette={palette} temperature={hero?.value} limit={spindleLimit} overLimit={!!hero?.overLimit} />
      {CELL_STATIONS.map((id) => (
        <Selectable key={id} id={id} onSelect={onSelect}>
          <StationCard id={id} palette={palette} selected={selected === id} />
          {id === "mill" && (
            <Mill
              palette={palette}
              heat={spindleHeat}
              rpm={spindleRpm}
              animate={animate}
              running={!held}
              maintenance={!!decisions.request_maintenance}
            />
          )}
          {id === "inspect" && (
            <VisionCamera palette={palette} animate={animate} requested={!!decisions.send_to_inspect} scanning={held} />
          )}
          {id === "pack" && <PackStation palette={palette} hold={batchHeld} />}
          <StackLight position={STACK_LIGHT_AT[id]} health={stations[id].health} attention={attention[id]} animate={animate} />
          <StationTag id={id} view={stations[id]} decision={decisionText(id)} />
        </Selectable>
      ))}
      {sensors.map((sensor) => {
        const style = calloutStyle(sensor, view, legendFor(sensors, view));
        return style && <SensorCallout key={sensor.name} sensor={sensor} style={style} limit={spindleLimit} />;
      })}
      {cue && [0, 0.35].map((delay) => <Ripple key={`${cue.at}-${delay}`} cue={cue} delay={delay} animate={animate} />)}
      <CameraRig view={view} instant={reducedMotion} />
      <OrbitControls
        makeDefault
        enableDamping
        enablePan={false}
        minDistance={4}
        maxDistance={42}
        minPolarAngle={0.35}
        maxPolarAngle={1.45}
      />
    </>
  );
}

export function MachiningCell(props: MachiningCellProps) {
  const { stations, selected, onSelect, animate, statusLabel, expanded, onToggleExpanded, stop, cue } = props;
  const theme = useResolvedTheme();
  const [view, setView] = useState<CellView>(selected);
  const [reducedMotion, setReducedMotion] = useState(
    () => typeof window !== "undefined" && window.matchMedia("(prefers-reduced-motion: reduce)").matches,
  );
  useEffect(() => {
    const media = window.matchMedia("(prefers-reduced-motion: reduce)");
    const sync = () => setReducedMotion(media.matches);
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);
  const running = animate && !reducedMotion;

  const cueAt = cue?.at;
  const cueView = cue?.view;
  useEffect(() => {
    if (cueView) setView(cueView);
  }, [cueAt, cueView]);

  const select = (station: CellStationId) => {
    onSelect(station);
    setView(station);
  };

  return (
    <div
      className={`machining-cell machining-cell--${theme}${stop?.phase === "stopped" ? " is-held" : ""}`}
      role="region"
      aria-label="Machining cell twin: mill, inspect, pack"
      data-motion={running ? "running" : "paused"}
      data-view={view}
      data-line={stop?.phase === "stopped" ? "stopped" : props.batchHeld ? "held-at-pack" : "moving"}
    >
      <Canvas
        shadows
        dpr={[1, 1.75]}
        camera={{ fov: 34, near: 0.1, far: 120, position: [0, 6, 18] }}
        gl={{ antialias: true, alpha: true, toneMapping: THREE.ACESFilmicToneMapping }}
      >
        <CellScene {...props} onSelect={select} animate={running} view={view} theme={theme} reducedMotion={reducedMotion} />
      </Canvas>
      <div className="machining-cell__top">
        <span className="machining-cell__status">{statusLabel}</span>
        <div className="machining-cell__views" role="group" aria-label="Camera view">
          <button type="button" aria-pressed={view === "cell"} onClick={() => setView("cell")}>
            Whole cell
          </button>
          <button type="button" aria-pressed={view === selected} onClick={() => setView(selected)}>
            {STATION_INFO[selected].name}
          </button>
          {onToggleExpanded && (
            <button type="button" className="machining-cell__expand" onClick={onToggleExpanded}>
              {expanded ? "Exit full view" : "Full view"}
            </button>
          )}
        </div>
      </div>
      {stop && (
        <div className={`machining-cell__hold is-${stop.phase}`} role="status" data-testid="cell-hold">
          <span className="machining-cell__hold-icon" aria-hidden="true" />
          {stop.phase === "stopped" ? (
            <div>
              <b>Production stopped · {stop.reason}</b>
              <span>
                Last machine reading {formatDeviceTime(stop.lastReading).slice(0, 8)} · CNC-1 and the pack line are
                silent
              </span>
            </div>
          ) : (
            <div>
              <b>Restarting the cell</b>
              <span>Resumed {formatDeviceTime(stop.since).slice(0, 8)} · waiting for the first CNC-1 reading</span>
            </div>
          )}
          {stop.phase === "stopped" && (
            <button type="button" onClick={stop.onResume} disabled={stop.resuming}>
              {stop.resuming ? "Resuming…" : "Resume production"}
            </button>
          )}
        </div>
      )}
      <SensorLegend sensors={props.sensors} view={view} />
      <nav className="machining-cell__stations" aria-label="Cell stations">
        {CELL_STATIONS.map((id) => {
          const info = STATION_INFO[id];
          const station = stations[id];
          return (
            <button
              key={id}
              type="button"
              className={selected === id ? "active" : ""}
              aria-pressed={selected === id}
              aria-label={`Select ${info.code} · ${info.name}`}
              onClick={() => select(id)}
            >
              <i className={`tone-${healthTone(station.health)}`} aria-hidden="true" />
              <span>{info.code}</span>
              {info.name}
              <em>{HEALTH_LABEL[station.health]}</em>
            </button>
          );
        })}
      </nav>
    </div>
  );
}
