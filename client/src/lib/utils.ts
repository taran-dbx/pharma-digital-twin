import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(value: number, decimals = 1): string {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  }).format(value);
}

export function formatLatency(ms: number): string {
  if (ms < 1) return "<1ms";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

/**
 * Device time as epoch seconds. Realtime writers send naive ISO strings in
 * UTC ("2026-10-20T16:23:32.504078"); producers send epoch seconds.
 */
export function toEpochSeconds(value: unknown): number {
  if (typeof value === "number") return value > 1e12 ? value / 1000 : value;
  if (typeof value !== "string" || value === "") return Number.NaN;
  const numeric = Number(value);
  if (Number.isFinite(numeric)) return toEpochSeconds(numeric);
  const zoned = /(Z|[+-]\d{2}:?\d{2})$/.test(value) ? value : `${value}Z`;
  return Date.parse(zoned) / 1000;
}

export function formatTimestamp(epoch: number): string {
  return new Date(epoch * 1000).toLocaleTimeString("en-US", {
    hour12: true,
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  });
}
