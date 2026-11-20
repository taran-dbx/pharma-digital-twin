import { useEffect, useState } from "react";

export type ResolvedTheme = "dark" | "light";

function readTheme(): ResolvedTheme {
  if (typeof document === "undefined") return "dark";
  const root = document.documentElement;
  return root.dataset.theme === "light" || root.classList.contains("light") ? "light" : "dark";
}

/** Follows the page theme (`data-theme` on <html>) for canvases that cannot use CSS variables. */
export function useResolvedTheme(): ResolvedTheme {
  const [theme, setTheme] = useState<ResolvedTheme>(readTheme);
  useEffect(() => {
    const observer = new MutationObserver(() => setTheme(readTheme()));
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class", "data-theme"] });
    return () => observer.disconnect();
  }, []);
  return theme;
}
