import type { Capabilities } from "../api/contracts";

let current: Capabilities | null = null;

export function setCapabilities(capabilities: Capabilities): void {
  current = capabilities;
}

export function getCapabilities(): Capabilities | null {
  return current;
}
