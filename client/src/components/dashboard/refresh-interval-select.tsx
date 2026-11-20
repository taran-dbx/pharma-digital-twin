import { useCallback, useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";

const STORAGE_KEY = "dashboard.refreshIntervalSec";

interface IntervalOption {
  value: number;
  label: string;
}

const OPTIONS: readonly IntervalOption[] = [
  { value: 0, label: "Off" },
  { value: 3, label: "3s" },
  { value: 5, label: "5s" },
  { value: 10, label: "10s" },
  { value: 30, label: "30s" },
  { value: 60, label: "1m" },
  { value: 300, label: "5m" },
];

function readStored(initial: number): number {
  if (typeof window === "undefined") return initial;
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (raw === null) return initial;
    const parsed = Number.parseInt(raw, 10);
    if (Number.isFinite(parsed) && OPTIONS.some((o) => o.value === parsed)) {
      return parsed;
    }
  } catch {
    // ignore — fall through to initial
  }
  return initial;
}

function writeStored(value: number): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(STORAGE_KEY, String(value));
  } catch {
    // ignore quota / privacy-mode errors
  }
}

/**
 * Reads the persisted refresh interval on mount and keeps it in sync with
 * ``localStorage`` on every setter call.  ``0`` means "paused".
 */
export function useRefreshInterval(
  initial = 30,
): [number, (next: number) => void] {
  const [value, setValue] = useState<number>(() => readStored(initial));

  const update = useCallback((next: number) => {
    setValue(next);
    writeStored(next);
  }, []);

  useEffect(() => {
    // Keep multi-tab instances roughly in sync.
    const onStorage = (e: StorageEvent) => {
      if (e.key !== STORAGE_KEY || e.newValue === null) return;
      const parsed = Number.parseInt(e.newValue, 10);
      if (Number.isFinite(parsed)) {
        setValue(parsed);
      }
    };
    window.addEventListener("storage", onStorage);
    return () => window.removeEventListener("storage", onStorage);
  }, []);

  return [value, update];
}

interface RefreshIntervalSelectProps {
  value: number;
  onChange: (secs: number) => void;
  onRefreshNow: () => void;
}

/**
 * Grafana-style refresh interval selector.  Emits a number of seconds
 * (``0`` = off) and exposes a manual "refresh now" button.
 */
export function RefreshIntervalSelect({
  value,
  onChange,
  onRefreshNow,
}: RefreshIntervalSelectProps) {
  const handleChange = useCallback(
    (raw: string) => {
      const next = Number.parseInt(raw, 10);
      if (Number.isFinite(next)) onChange(next);
    },
    [onChange],
  );

  return (
    <div className="flex items-center gap-1.5">
      <Select value={String(value)} onValueChange={handleChange}>
        <SelectTrigger
          className="h-8 w-[92px] px-2 text-xs"
          aria-label="Refresh interval"
        >
          <SelectValue placeholder="Interval" />
        </SelectTrigger>
        <SelectContent>
          {OPTIONS.map((o) => (
            <SelectItem key={o.value} value={String(o.value)}>
              {o.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Button
        type="button"
        variant="outline"
        size="icon"
        className="h-8 w-8"
        onClick={onRefreshNow}
        title="Refresh now"
        aria-label="Refresh now"
      >
        <RefreshCw className="h-3.5 w-3.5" />
      </Button>
    </div>
  );
}
