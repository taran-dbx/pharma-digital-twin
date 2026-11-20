/**
 * Shared chrome for the Genie investigation drawer: right-side panel with a
 * toggle, header + status pill, scrollable transcript, and composer. Both the
 * workspace-wide and per-pipeline copilots render inside this shell so their
 * selectors (`genie-copilot*`) and layout stay identical.
 */
import { useEffect, useRef, type FormEvent, type KeyboardEvent, type ReactNode } from "react";
import { PanelRight, Send } from "lucide-react";

interface GenieDrawerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  subtitle: string;
  statusText: string;
  /** Banners (loading / disabled / error) and prompt chips above the transcript. */
  notices: ReactNode;
  /** Transcript content. */
  children: ReactNode;
  composer: ReactNode;
  /**
   * Plain text announced to screen readers (latest answer). The transcript
   * itself is not a live region, so charts and tables are never read out.
   */
  announcement?: string;
}

export function GenieDrawer({
  open,
  onOpenChange,
  subtitle,
  statusText,
  notices,
  children,
  composer,
  announcement = "",
}: GenieDrawerProps) {
  const toggleRef = useRef<HTMLButtonElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const opener = useRef<HTMLElement | null>(null);
  const wasOpen = useRef(open);

  // Non-modal focus management: move focus in on open, hand it back on close.
  useEffect(() => {
    if (open === wasOpen.current) return;
    wasOpen.current = open;
    const panel = panelRef.current;
    if (open) {
      const active = document.activeElement;
      opener.current = active instanceof HTMLElement && active !== document.body ? active : toggleRef.current;
      const input = panel?.querySelector<HTMLElement>("textarea:not(:disabled)");
      (input ?? panel)?.focus();
      return;
    }
    const target = opener.current?.isConnected ? opener.current : toggleRef.current;
    const focusInside = panel?.contains(document.activeElement) ?? false;
    if (focusInside || document.activeElement === document.body) target?.focus();
    opener.current = null;
  }, [open]);

  function handleKeyDown(event: KeyboardEvent<HTMLElement>) {
    if (event.key === "Escape" && open && !event.defaultPrevented) {
      event.stopPropagation();
      onOpenChange(false);
    }
  }

  return (
    <aside
      className={`genie-drawer${open ? " is-open" : ""}`}
      data-testid="genie-copilot"
      aria-label="Genie investigation"
      onKeyDown={handleKeyDown}
    >
      <button
        ref={toggleRef}
        type="button"
        className="genie-drawer__toggle"
        data-testid="genie-copilot-toggle"
        aria-expanded={open}
        onClick={() => onOpenChange(!open)}
      >
        <PanelRight size={16} aria-hidden="true" />
        <span>Investigate</span>
      </button>
      <div
        className="genie-drawer__panel"
        data-testid="genie-copilot-drawer"
        hidden={!open}
        ref={panelRef}
        tabIndex={-1}
      >
        <header className="genie-drawer__head">
          <div>
            <h2>Genie investigation</h2>
            <p title={subtitle}>{subtitle}</p>
          </div>
          <span className="genie-drawer__status" data-testid="genie-status" aria-live="polite">
            {statusText}
          </span>
        </header>

        {notices}

        <div className="genie-drawer__transcript" data-testid="genie-copilot-transcript">
          {children}
        </div>
        <p className="sr-only" aria-live="polite" data-testid="genie-announcement">
          {announcement}
        </p>

        {composer}
      </div>
    </aside>
  );
}

interface GenieComposerProps {
  value: string;
  onChange: (value: string) => void;
  onSubmit: (value: string) => void;
  disabled: boolean;
  placeholder?: string;
}

export function GenieComposer({
  value,
  onChange,
  onSubmit,
  disabled,
  placeholder = "Investigate this twin…",
}: GenieComposerProps) {
  function handleSubmit(event: FormEvent) {
    event.preventDefault();
    onSubmit(value);
  }

  return (
    <form className="genie-drawer__composer" onSubmit={handleSubmit}>
      <label className="sr-only" htmlFor="genie-question">
        Investigation question
      </label>
      <textarea
        id="genie-question"
        data-testid="genie-copilot-input"
        rows={3}
        value={value}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        placeholder={placeholder}
      />
      <button
        type="submit"
        className="btn primary sm"
        data-testid="genie-copilot-send"
        disabled={disabled || !value.trim()}
      >
        <Send size={14} aria-hidden="true" />
        Send
      </button>
    </form>
  );
}
