import { Sun, Moon, User } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useUser } from "@/contexts/user-context";

interface HeaderProps {
  theme: "dark" | "light";
  onToggleTheme: () => void;
  title?: string;
}

export function Header({
  theme,
  onToggleTheme,
  title,
}: HeaderProps) {
  const { email } = useUser();

  return (
    <header className="sticky top-0 z-30 flex h-14 items-center justify-between border-b border-border/50 bg-surface-low/80 px-6 backdrop-blur-xl">
      <div className="flex items-center gap-3">
        {title && (
          <h1 className="font-display text-base font-semibold tracking-tight">{title}</h1>
        )}
      </div>

      <div className="flex items-center gap-3">
        {email && email !== "anonymous" && (
          <div className="flex items-center gap-1.5 rounded-full border border-border/50 bg-surface-high/50 px-3 py-1 font-ui text-xs text-muted-foreground">
            <User className="h-3 w-3" />
            <span className="max-w-[180px] truncate">{email}</span>
          </div>
        )}

        <Button
          variant="ghost"
          size="icon"
          onClick={onToggleTheme}
          aria-label="Toggle theme"
        >
          {theme === "dark" ? (
            <Sun className="h-4 w-4" />
          ) : (
            <Moon className="h-4 w-4" />
          )}
        </Button>
      </div>
    </header>
  );
}
