import { useState } from "react";
import { Outlet, useLocation } from "react-router-dom";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Sidebar } from "./sidebar";
import { Header } from "./header";
import { useTheme } from "@/hooks/use-theme";
import { cn } from "@/lib/utils";

function resolveTitle(pathname: string): string {
  if (pathname === "/") return "Home";
  if (pathname === "/pipelines/new") return "Create Pipeline";
  if (/^\/pipelines\/[^/]+\/dashboard$/.test(pathname)) return "Pipeline Dashboard";
  if (/^\/pipelines\/[^/]+$/.test(pathname)) return "Pipeline Details";
  if (pathname === "/settings") return "Settings";
  return "IoT Digital Twin";
}

export function AppShell() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const { theme, toggleTheme } = useTheme();
  const location = useLocation();

  const title = resolveTitle(location.pathname);

  return (
    <TooltipProvider>
      <div className="flex min-h-screen bg-background">
        <Sidebar
          collapsed={sidebarCollapsed}
          onToggle={() => setSidebarCollapsed((prev) => !prev)}
        />

        <div
          className={cn(
            "flex flex-1 flex-col transition-all duration-300",
            sidebarCollapsed ? "ml-16" : "ml-56",
          )}
        >
          <Header
            theme={theme}
            onToggleTheme={toggleTheme}
            title={title}
          />
          <main className="flex-1 p-6">
            <Outlet />
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}
