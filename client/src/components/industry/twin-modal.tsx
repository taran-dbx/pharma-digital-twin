/**
 * Shared modal overlay — ports `#tw-modal` / `openModal()` / `closeModal()`
 * from the original design reference (kept outside this repo).
 * The design renders this once as a sibling of `.app-shell` and reuses it
 * for both the "View data flow" (`.fc-info` on a pipeline card) and "View
 * sample" (setup screen's `su-sample` button) triggers — same here via a
 * small context so any page under `<TwinShell>` can call `useTwinModal()`.
 */
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

interface ModalState {
  title: string;
  body: ReactNode;
}

interface TwinModalContextValue {
  open: (title: string, body: ReactNode) => void;
  close: () => void;
}

const TwinModalContext = createContext<TwinModalContextValue | null>(null);

export function useTwinModal(): TwinModalContextValue {
  const ctx = useContext(TwinModalContext);
  if (!ctx) {
    throw new Error("useTwinModal must be used inside <TwinModalProvider>");
  }
  return ctx;
}

export function TwinModalProvider({ children }: { children: ReactNode }) {
  const [modal, setModal] = useState<ModalState | null>(null);

  const open = useCallback((title: string, body: ReactNode) => setModal({ title, body }), []);
  const close = useCallback(() => setModal(null), []);

  // Ported from the design's `document.addEventListener('keydown', ...)`.
  useEffect(() => {
    if (!modal) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [modal, close]);

  const value = useMemo(() => ({ open, close }), [open, close]);

  return (
    <TwinModalContext.Provider value={value}>
      {children}
      <div
        className={`modal-ov ${modal ? "on" : ""}`}
        onClick={(e) => {
          if (e.target === e.currentTarget) close();
        }}
      >
        <div className="modal">
          <div className="modal-hd">
            <h3>{modal?.title ?? "—"}</h3>
            <button className="modal-x" title="Close" onClick={close}>
              <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth={1.6} aria-hidden="true">
                <path d="M4 4l8 8M12 4l-8 8" />
              </svg>
            </button>
          </div>
          <div className="modal-bd">{modal?.body}</div>
        </div>
      </div>
    </TwinModalContext.Provider>
  );
}
