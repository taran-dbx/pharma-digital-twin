import * as THREE from "three";
import { DIMS, PIPE_TOP } from "./materials";

const { rx, rz, rH } = DIMS;
const V = (x: number, y: number, z: number) => new THREE.Vector3(x, y, z);

export interface PipeSegment {
  /** start point */
  a: THREE.Vector3;
  /** end point */
  b: THREE.Vector3;
  /** segment length */
  len: number;
  /** midpoint (cylinder centre) */
  mid: THREE.Vector3;
  /** orientation quaternion (Y-up cylinder → segment direction) */
  quat: THREE.Quaternion;
}

export interface PipeRoute {
  color: number;
  /** straight runs between elbows */
  segments: PipeSegment[];
  /** elbow joint positions (every interior + end vertex) */
  elbows: THREE.Vector3[];
  /** valve handwheel position (40% along first segment, lifted slightly) */
  valve: THREE.Vector3;
  /** total run length (used to animate the flow particle) */
  total: number;
  /** whether a flowing particle travels this route */
  withFlow: boolean;
}

/** Build a routed pipe (straight runs + elbows + valve) from ordered points. */
function buildRoute(
  points: THREE.Vector3[],
  color: number,
  withFlow: boolean,
): PipeRoute {
  const segments: PipeSegment[] = [];
  const elbows: THREE.Vector3[] = [];
  for (let i = 0; i < points.length - 1; i++) {
    const a = points[i];
    const b = points[i + 1];
    const dir = new THREE.Vector3().subVectors(b, a);
    const len = dir.length();
    const mid = a.clone().add(b).multiplyScalar(0.5);
    const quat = new THREE.Quaternion().setFromUnitVectors(
      new THREE.Vector3(0, 1, 0),
      dir.clone().normalize(),
    );
    segments.push({ a, b, len, mid, quat });
    elbows.push(b.clone());
  }
  const valve = points[0].clone().lerp(points[1], 0.4);
  valve.y += 0.18;
  const total = segments.reduce((s, seg) => s + seg.len, 0);
  return { color, segments, elbows, valve, total, withFlow };
}

const top = PIPE_TOP;

/** The four routed services, ported verbatim from the mockup. */
export const ROUTES: PipeRoute[] = [
  // WFI → reactor (carries the animated flow particle)
  buildRoute(
    [
      V(-4.5, 3.6, 2.2),
      V(-4.5, top, 2.2),
      V(rx, top, 2.2),
      V(rx, top, rz),
      V(rx, rH + 0.6, rz),
    ],
    0x4a90d9,
    true,
  ),
  // reactor → filtration (product)
  buildRoute(
    [
      V(rx, rH + 0.4, rz),
      V(rx, top, rz),
      V(rx, top, -2.2),
      V(4.2, top, -2.2),
      V(4.2, 2.6, -2.2),
    ],
    0x33d27e,
    false,
  ),
  // steam header
  buildRoute(
    [
      V(-4.5, 2.6, -2.2),
      V(-4.5, top - 0.6, -2.2),
      V(4.6, top - 0.6, -2.2),
      V(4.6, 2.6, 2.2),
    ],
    0xf59e0b,
    false,
  ),
  // CIP → fill line
  buildRoute(
    [
      V(4.6, 2.6, 2.2),
      V(4.6, top - 1.0, 2.2),
      V(rx, top - 1.0, 2.2),
      V(rx, top - 1.0, 3.3),
      V(1.6, top - 1.0, 3.3),
      V(1.6, 0.8, 3.3),
    ],
    0xa855f7,
    false,
  ),
];

/** Position of the flow particle at run-distance `d` along a route. */
export function flowPosition(route: PipeRoute, d: number): THREE.Vector3 {
  let dist = d % route.total;
  for (const seg of route.segments) {
    if (dist <= seg.len) {
      return seg.a.clone().lerp(seg.b, dist / seg.len);
    }
    dist -= seg.len;
  }
  const last = route.segments[route.segments.length - 1];
  return last.b.clone();
}
