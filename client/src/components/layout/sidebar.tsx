import { NavLink } from "react-router-dom";
import {
  ChevronLeft,
  ChevronRight,
  Activity,
  Home,
  Plus,
  Settings,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface SidebarProps {
  collapsed: boolean;
  onToggle: () => void;
}

// Dashboards are per-pipeline — open one from a pipeline card, not the sidebar.
const navItems = [
  { to: "/", label: "Home", icon: Home },
  { to: "/pipelines/new", label: "Create Pipeline", icon: Plus },
  { to: "/settings", label: "Settings", icon: Settings },
];

export function Sidebar({ collapsed, onToggle }: SidebarProps) {
  return (
    <aside
      className={cn(
        "fixed left-0 top-0 z-40 flex h-screen flex-col border-r border-border/50 bg-surface-low transition-all duration-300",
        collapsed ? "w-16" : "w-56",
      )}
    >
      <div className="flex h-14 items-center gap-2 border-b border-border/50 px-4">
        <Activity className="h-5 w-5 shrink-0 text-primary" />
        {!collapsed && (
          <span className="font-display text-sm font-bold tracking-tight">
            IoT Digital Twin
          </span>
        )}
      </div>

      <nav className="flex-1 space-y-1 px-2 py-4">
        {navItems.map((item) => {
          const link = (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.to === "/"}
              className={({ isActive }) =>
                cn(
                  "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium font-ui transition-colors",
                  isActive
                    ? "bg-primary/15 text-primary glow-primary"
                    : "text-muted-foreground hover:bg-surface-high hover:text-foreground",
                )
              }
            >
              <item.icon className="h-4 w-4 shrink-0" />
              {!collapsed && <span>{item.label}</span>}
            </NavLink>
          );

          if (collapsed) {
            return (
              <Tooltip key={item.to} delayDuration={0}>
                <TooltipTrigger asChild>{link}</TooltipTrigger>
                <TooltipContent side="right">{item.label}</TooltipContent>
              </Tooltip>
            );
          }
          return link;
        })}
      </nav>

      <div className="border-t border-border/50 p-2">
        <Button
          variant="ghost"
          size="icon"
          onClick={onToggle}
          className="w-full"
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          {collapsed ? (
            <ChevronRight className="h-4 w-4" />
          ) : (
            <ChevronLeft className="h-4 w-4" />
          )}
        </Button>
      </div>
    </aside>
  );
}
