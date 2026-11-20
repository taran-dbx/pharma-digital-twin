import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { FactoryPrototype } from "./factory-prototype";
import "./styles/dubois.css";
import "./factory-prototype.css";

const root = document.getElementById("factory-prototype-root");
if (!root) throw new Error("Factory prototype root element was not found");

createRoot(root).render(
  <StrictMode>
    <FactoryPrototype />
  </StrictMode>,
);
