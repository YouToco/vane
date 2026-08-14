import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { installChunkLoadRecovery } from "@/shared/runtime/chunk-load-recovery";
import { ChunkLoadErrorBoundary } from "@/app/ChunkLoadErrorBoundary";
import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { I18nProvider } from "@/i18n";
import "./index.css";

installChunkLoadRecovery();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <ChunkLoadErrorBoundary>
      <I18nProvider>
        <TooltipProvider>
          <App />
          <Toaster richColors position="top-right" />
        </TooltipProvider>
      </I18nProvider>
    </ChunkLoadErrorBoundary>
  </StrictMode>,
);
