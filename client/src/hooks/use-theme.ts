import { useState, useEffect, useCallback } from "react";

type Theme = "dark" | "light";

export function useTheme() {
  const [theme, setThemeState] = useState<Theme>(() => {
    if (typeof window === "undefined") return "dark";
    const stored = localStorage.getItem("iot-twin-theme") as Theme | null;
    return stored || "dark";
  });

  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove("dark", "light");
    root.classList.add(theme);
    // The DuBois tier palette (`client/src/styles/dubois.css`) reacts to
    // `[data-theme="dark"]` — same attribute the design reference toggles
    // (the original design reference's `applyTheme()`) — in
    // addition to the `.dark` class the legacy Tailwind chrome uses.
    root.setAttribute("data-theme", theme);
    localStorage.setItem("iot-twin-theme", theme);
  }, [theme]);

  const toggleTheme = useCallback(() => {
    setThemeState((prev) => (prev === "dark" ? "light" : "dark"));
  }, []);

  return { theme, toggleTheme };
}
