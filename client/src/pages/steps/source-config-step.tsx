import { SourceSettingsPage } from "@/pages/source-settings-page";

import type { PipelineResources, PipelineType } from "@/lib/types";

export interface SourceConfigStepProps {
  pipelineId: string | null;
  pipelineResources?: PipelineResources;
  onValidityChange?: (valid: boolean) => void;
  onJobRunStateChange?: (state: "idle" | "polling" | "running" | "stopped" | "error") => void;
  onPipelineTypeChange?: (pipelineType: PipelineType) => void;
}

export function SourceConfigStep({ pipelineId, pipelineResources, onValidityChange, onJobRunStateChange, onPipelineTypeChange }: SourceConfigStepProps) {
  return (
    <SourceSettingsPage
      pipelineId={pipelineId}
      pipelineResources={pipelineResources}
      onValidityChange={onValidityChange}
      onJobRunStateChange={onJobRunStateChange}
      onPipelineTypeChange={onPipelineTypeChange}
    />
  );
}
