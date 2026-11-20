import * as THREE from "three";

/** Shared scene dimensions ported from the approved mockup. */
export const DIMS = {
  FX: 6.5,
  FZ: 4,
  FY: 5.2,
  /** bioreactor */
  rx: 0.4,
  rz: 0,
  rR: 1.35,
  rH: 3.2,
} as const;

/** Top routing plane for the pipe network. */
export const PIPE_TOP = DIMS.FY * 0.55 + 1.0;

/** Pipe-network colours, keyed by service. */
export const PIPE_COL = {
  product: 0x33d27e,
  wfi: 0x4a90d9,
  steam: 0xf59e0b,
  cip: 0xa855f7,
} as const;

/** Standard steel material factory (mirrors the mockup `steel()` helper). */
export function steel(color: number, metalness = 0.85, roughness = 0.3) {
  return new THREE.MeshStandardMaterial({ color, metalness, roughness });
}

/**
 * Temperature → liquid colour ramp (blue → green → amber → red),
 * ported verbatim from the mockup `lerpC()`.
 */
export function tempColor(t: number): THREE.Color {
  const a = new THREE.Color(0x2563eb);
  const b = new THREE.Color(0x33d27e);
  const c = new THREE.Color(0xf59e0b);
  const d = new THREE.Color(0xef4444);
  if (t < 0.4) return a.clone().lerp(b, t / 0.4);
  if (t < 0.7) return b.clone().lerp(c, (t - 0.4) / 0.3);
  return c.clone().lerp(d, (t - 0.7) / 0.3);
}
