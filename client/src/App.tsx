import { BrowserRouter, Navigate, Routes, Route } from "react-router-dom";
import { Toaster } from "sonner";

import { SensorProvider } from "@/contexts/sensor-context";
import { UserProvider } from "@/contexts/user-context";
import { PipelineProvider } from "@/contexts/pipeline-context";
import { AppShell } from "@/components/layout/app-shell";
import { TwinShell } from "@/components/industry/twin-shell";
import { IndustryGalleryPage } from "@/pages/industry-gallery-page";
import { PlatformLandingPage } from "@/pages/platform-landing-page";
import { IndustrySetupPage } from "@/pages/industry-setup-page";
import { PipelineSetupValidationPage } from "@/pages/pipeline-setup-validation-page";
import { PipelineManagementPage } from "@/pages/pipeline-management-page";
import { PipelineHealthPage } from "@/pages/pipeline-health-page";
import { HealthPreviewPage } from "@/pages/_health-preview";
import { ModeDashboardPage } from "@/pages/mode-dashboard-page";
import { SettingsPage } from "@/pages/settings-page";

export default function App() {
  return (
    <BrowserRouter>
      <UserProvider>
        <PipelineProvider>
          <SensorProvider>
            <Routes>
              <Route element={<TwinShell />}>
                <Route path="/" element={<PlatformLandingPage />} />
                <Route path="/industries" element={<IndustryGalleryPage />} />
                <Route path="/i/:industry" element={<IndustrySetupPage />} />
                <Route path="/i/:industry/:mode/dashboard" element={<ModeDashboardPage />} />
                <Route path="/legacy" element={<Navigate to="/industries" replace />} />
                <Route path="/pipelines/new" element={<Navigate to="/setup-validate" replace />} />
                <Route path="/setup-validate" element={<PipelineSetupValidationPage />} />
                <Route path="/pipelines" element={<PipelineManagementPage />} />
                <Route path="/pipelines/:id/health" element={<PipelineHealthPage />} />
                <Route path="/health-preview" element={<HealthPreviewPage />} />
                <Route path="/pipelines/:id/dashboard" element={<Navigate to="/industries" replace />} />
                <Route path="/pipelines/:id" element={<Navigate to="/industries" replace />} />
              </Route>
              <Route element={<AppShell />}>
                <Route path="/dashboard" element={<Navigate to="/industries" replace />} />
                <Route path="/settings" element={<SettingsPage />} />
              </Route>
            </Routes>
            <Toaster
              position="bottom-right"
              toastOptions={{
                className: "bg-card text-card-foreground border-border",
              }}
            />
          </SensorProvider>
        </PipelineProvider>
      </UserProvider>
    </BrowserRouter>
  );
}
