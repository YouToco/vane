import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "@/app/index.css";
import "./prototype.css";
import PrototypeTaskDetail from "./PrototypeTaskDetail";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <PrototypeTaskDetail />
  </StrictMode>,
);
