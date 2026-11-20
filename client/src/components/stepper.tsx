import { Check } from "lucide-react";
import { cn } from "@/lib/utils";

export interface StepperProps {
  steps: string[];
  currentStep: number;
}

export function Stepper({ steps, currentStep }: StepperProps) {
  return (
    <nav
      aria-label="Pipeline creation steps"
      className="mb-8 max-w-full overflow-x-auto pb-2"
    >
      <ol className="flex w-max min-w-full items-center gap-2">
        {steps.map((label, i) => {
          const completed = i < currentStep;
          const active = i === currentStep;
          return (
            <li key={label} className="flex shrink-0 items-center gap-2">
              {i > 0 && (
                <div
                  className={cn(
                    "h-px w-8 sm:w-16",
                    completed ? "bg-primary" : "bg-border/50",
                  )}
                />
              )}
              <div className="flex items-center gap-2">
                <div
                  className={cn(
                    "flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-xs font-bold transition-colors",
                    completed
                      ? "bg-success text-primary-foreground"
                      : active
                        ? "bg-primary text-primary-foreground shadow-[0_0_10px_rgba(255,54,33,0.4)]"
                        : "bg-surface-high border border-border/50 text-muted-foreground",
                  )}
                >
                  {completed ? <Check className="h-4 w-4" /> : i + 1}
                </div>
                <span
                  className={cn(
                    "hidden font-ui text-xs font-medium sm:inline",
                    active
                      ? "text-foreground"
                      : completed
                        ? "text-primary"
                        : "text-muted-foreground",
                  )}
                >
                  {label}
                </span>
              </div>
            </li>
          );
        })}
      </ol>
    </nav>
  );
}
