import {
  createContext,
  useContext,
  useState,
  useCallback,
  useEffect,
  type ReactNode,
} from "react";
import { api } from "@/lib/api";
import type { Pipeline } from "@/lib/types";

interface PipelineContextValue {
  pipelines: Pipeline[];
  loading: boolean;
  refresh: () => Promise<void>;
  createPipeline: (name: string, industry?: string) => Promise<Pipeline>;
  updatePipeline: (id: string, data: Partial<Pipeline>) => Promise<Pipeline>;
  deletePipeline: (id: string) => Promise<void>;
  clonePipeline: (id: string, name: string) => Promise<Pipeline>;
}

const PipelineContext = createContext<PipelineContextValue>({
  pipelines: [],
  loading: true,
  refresh: async () => {},
  createPipeline: async () => ({}) as Pipeline,
  updatePipeline: async () => ({}) as Pipeline,
  deletePipeline: async () => {},
  clonePipeline: async () => ({}) as Pipeline,
});

export function PipelineProvider({ children }: { children: ReactNode }) {
  const [pipelines, setPipelines] = useState<Pipeline[]>([]);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const data = await api.listPipelines();
      setPipelines(Array.isArray(data) ? data : []);
    } catch {
      setPipelines([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const createPipeline = useCallback(
    async (name: string, industry?: string) => {
      const pipeline = await api.createPipeline({ name, industry });
      setPipelines((prev) => [pipeline, ...prev]);
      return pipeline;
    },
    [],
  );

  const updatePipeline = useCallback(
    async (id: string, data: Partial<Pipeline>) => {
      const updated = await api.updatePipeline(id, data);
      setPipelines((prev) =>
        prev.map((p) => (p.id === id ? updated : p)),
      );
      return updated;
    },
    [],
  );

  const deletePipeline = useCallback(
    async (id: string) => {
      setPipelines((prev) => prev.filter((p) => p.id !== id));
      try {
        await api.deletePipeline(id);
      } catch {
        // Backend cleanup may fail/timeout but pipeline is already marked
        // deleted in DB (happens before cleanup), so it won't reappear.
      }
    },
    [],
  );

  const clonePipeline = useCallback(
    async (id: string, name: string) => {
      const cloned = await api.clonePipeline(id, name);
      setPipelines((prev) => [cloned, ...prev]);
      return cloned;
    },
    [],
  );

  return (
    <PipelineContext.Provider
      value={{
        pipelines,
        loading,
        refresh,
        createPipeline,
        updatePipeline,
        deletePipeline,
        clonePipeline,
      }}
    >
      {children}
    </PipelineContext.Provider>
  );
}

export function usePipelines() {
  return useContext(PipelineContext);
}
