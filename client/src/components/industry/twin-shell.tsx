import { Link, Outlet } from "react-router-dom";

import { useTheme } from "@/hooks/use-theme";
import { useUser } from "@/contexts/user-context";
import { TwinModalProvider } from "./twin-modal";

function ThemeIcon({ theme }: { theme: "dark" | "light" }) {
  if (theme === "dark") {
    return (
      <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.5} aria-hidden="true">
        <circle cx="8" cy="8" r="3" />
        <path d="M8 1v1.5M8 13.5V15M1 8h1.5M13.5 8H15M3 3l1 1M12 12l1 1M13 3l-1 1M4 12l-1 1" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.5} aria-hidden="true">
      <path d="M14 8.5A6 6 0 1 1 7.5 2a4.7 4.7 0 0 0 6.5 6.5z" />
    </svg>
  );
}

function initialsFromEmail(email: string): string {
  const local = email.split("@")[0] ?? "";
  const parts = local.split(/[._-]+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  if (local.length >= 2) return local.slice(0, 2).toUpperCase();
  return (local || "?").toUpperCase();
}

function TopBar() {
  const { theme, toggleTheme } = useTheme();
  const { email, isAdmin } = useUser();
  const displayEmail = email && email !== "anonymous" ? email : "you@databricks.com";

  return (
    <div className="top-bar">
      <Link className="brand" to="/" aria-label="IoT Digital Twin home">
        <svg className="mark" viewBox="0 0 105 113" aria-hidden="true">
          <path
            d="M98.9 46.6 L52.3 72.9 L2.4 44.8 L0 46.1 V66.5 L52.3 95.9 L98.9 69.7 V80.5 L52.3 106.8 L2.4 78.7 L0 80 V83.5 L52.3 112.9 L104.5 83.5 V63.1 L102.1 61.8 L52.3 89.8 L5.6 63.6 V52.8 L52.3 79 L104.5 49.6 V29.5 L101.9 28 L52.3 55.9 L8 31.1 L52.3 6.2 L88.7 26.7 L91.9 24.9 V22.4 L52.3 0.1 L0 29.5 V32.7 L52.3 62.1 L98.9 35.8 Z"
            fill="#FF3621"
          />
        </svg>
        <span className="name">Databricks</span>
      </Link>
      <div className="tb-spacer" />
      <div className="actions">
        {isAdmin && (
          <Link className="btn ghost sm" to="/pipelines" title="Manage all deployed pipelines">
            <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.4} aria-hidden="true">
              <rect x="2" y="2.5" width="12" height="3" rx="1" />
              <rect x="2" y="6.5" width="12" height="3" rx="1" />
              <rect x="2" y="10.5" width="12" height="3" rx="1" />
            </svg>
            All pipelines
          </Link>
        )}
        <button
          className="ic-btn"
          title="Toggle light / dark"
          aria-label="Toggle light / dark"
          aria-pressed={theme === "dark"}
          onClick={toggleTheme}
        >
          <ThemeIcon theme={theme} />
        </button>
        <div className="avatar" title={displayEmail}>
          {initialsFromEmail(displayEmail)}
        </div>
      </div>
    </div>
  );
}

export function TwinShell() {
  return (
    <TwinModalProvider>
      <div className="app-shell">
        <TopBar />
        <main className="main-area">
          <div className="main-panel">
            <div className="wrap">
              <Outlet />
            </div>
          </div>
        </main>
      </div>
    </TwinModalProvider>
  );
}
