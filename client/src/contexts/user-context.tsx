import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";
import { api } from "@/lib/api";
import { getCatalog } from "@/lib/industries-api";

/**
 * Signed-in user context. The admin flag is decided *server-side*
 * (`services/app_permissions.py`, group `iot-admins` / `ADMIN_EMAILS`) and
 * surfaced per request as `Catalog.isAdmin` — there is no client-side role
 * override. `isAdmin` here is that server verdict, fetched once from the
 * industry catalog so the top bar can gate admin-only chrome (e.g. the
 * "All pipelines" nav button) before any page-level catalog load resolves.
 */
interface UserContextValue {
  email: string;
  loading: boolean;
  /** Server-decided admin flag (`iot-admins` group / `ADMIN_EMAILS`). */
  isAdmin: boolean;
  /**
   * Given a per-page server `isAdmin` value, return the effective admin flag.
   * A passthrough today — the sole gate is the server value — kept so pages
   * that read the catalog's own `is_admin` stay decoupled from the shell.
   */
  resolveIsAdmin: (serverIsAdmin: boolean) => boolean;
}

const UserContext = createContext<UserContextValue>({
  email: "",
  loading: true,
  isAdmin: false,
  resolveIsAdmin: (serverIsAdmin) => serverIsAdmin,
});

export function UserProvider({ children }: { children: ReactNode }) {
  const [email, setEmail] = useState("");
  const [loading, setLoading] = useState(true);
  const [isAdmin, setIsAdmin] = useState(false);

  useEffect(() => {
    let cancelled = false;
    api
      .getMe()
      .then((data) => {
        if (!cancelled) setEmail(data.email);
      })
      .catch(() => {
        if (!cancelled) setEmail("anonymous");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Resolve the server admin flag once for the shell. Best-effort: a 401/503
  // (no session / no backend) leaves `isAdmin` false, i.e. the end-user view.
  useEffect(() => {
    let cancelled = false;
    getCatalog()
      .then((data) => {
        if (!cancelled) setIsAdmin(data.isAdmin);
      })
      .catch(() => {
        /* not admin / no backend — stay false */
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const resolveIsAdmin = useCallback((serverIsAdmin: boolean) => serverIsAdmin, []);

  return (
    <UserContext.Provider value={{ email, loading, isAdmin, resolveIsAdmin }}>
      {children}
    </UserContext.Provider>
  );
}

export function useUser() {
  return useContext(UserContext);
}
