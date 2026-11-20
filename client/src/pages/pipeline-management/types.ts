/**
 * Prototype data + types for the admin "Pipelines Management" list page.
 * Local-only placeholder data — no backend calls. Three sample industry
 * pipelines with deliberately distinct health states (healthy / degraded /
 * down) so every status treatment is visible on first paint.
 */
import type { LucideIcon } from "lucide-react";

/** Streaming flavor a pipeline lane runs in. Mirrors the setup flow's modes. */
export type PipelineMode = "realtime" | "microbatch" | "zerobus";

/** Overall roll-up health for a deployed pipeline. `draft` = setup/validation
 * not complete yet (not startable). */
export type Health = "healthy" | "degraded" | "down" | "draft";

export interface DeployedPipeline {
  /** URL id — the setup id, feeds `/pipelines/{id}/health`. */
  id: string;
  /** Raw industry id (e.g. `pharmaceutical`) — feeds `/setup-validate?industry=`. */
  industryId?: string;
  industry: string;
  /** Short "what it monitors" line, matching the home cards' sub-text. */
  tagline: string;
  icon: LucideIcon;
  /** Which streaming lanes are enabled for this industry. */
  modes: PipelineMode[];
  health: Health;
  /** False when the pipeline can't be started yet (draft / not provisioned). */
  startable: boolean;
  jobsRunning: number;
  jobsTotal: number;
  resourcesHealthy: number;
  resourcesTotal: number;
  lastUpdated: string;
  /** Optional one-liner surfaced for draft / degraded / down states. */
  note?: string;
}

export const MODE_LABEL: Record<PipelineMode, string> = {
  realtime: "Real-time",
  microbatch: "Micro-batch",
  zerobus: "Zerobus",
};
