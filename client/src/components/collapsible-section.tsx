import { useState } from "react";
import * as Collapsible from "@radix-ui/react-collapsible";
import { CheckCircle2, ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";
import type { LucideIcon } from "lucide-react";

interface CollapsibleSectionProps {
  title: string;
  icon?: LucideIcon;
  complete?: boolean;
  defaultOpen?: boolean;
  children: React.ReactNode;
}

export function CollapsibleSection({
  title,
  icon: Icon,
  complete = false,
  defaultOpen = false,
  children,
}: CollapsibleSectionProps) {
  const [open, setOpen] = useState(defaultOpen);

  return (
    <Collapsible.Root open={open} onOpenChange={setOpen}>
      <Collapsible.Trigger asChild>
        <button
          type="button"
          className="flex w-full items-center gap-2 rounded-md border border-border/50 bg-surface-high px-3 py-2.5 transition-colors hover:bg-surface-highest"
        >
          {Icon && (
            <Icon className="h-4 w-4 shrink-0 text-muted-foreground" />
          )}
          <h2 className="font-display text-sm font-semibold uppercase tracking-wider text-foreground">
            {title}
          </h2>
          {complete ? (
            <CheckCircle2 className="h-4 w-4 shrink-0 text-success" />
          ) : (
            <span className="h-4 w-4 shrink-0" />
          )}
          <ChevronDown
            className={cn(
              "ml-auto h-4 w-4 shrink-0 text-muted-foreground transition-transform duration-200",
              open && "rotate-180",
            )}
          />
        </button>
      </Collapsible.Trigger>
      <Collapsible.Content className="overflow-hidden data-[state=closed]:animate-collapse data-[state=open]:animate-expand">
        <div className="pt-3">{children}</div>
      </Collapsible.Content>
    </Collapsible.Root>
  );
}
