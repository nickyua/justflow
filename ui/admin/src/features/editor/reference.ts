import type {
  AuthoringReference,
  AuthoringReferenceComponent,
  AuthoringReferenceService,
} from "../../api/contracts";
import { client } from "../../app/api";
import {
  appendCell,
  appendNodeCell,
  clear,
  element,
  emptyTable,
  input,
  textNode,
} from "../../app/dom";
import { safeError } from "../../app/format";
import { setView } from "../../app/router";

const COMPONENTS_SERVICES = "component-services";
const COMPONENTS_RESOURCES = "component-resources";
const FRAGMENT_SERVICES = "fragment-component-services";
const FRAGMENT_RESOURCES = "fragment-component-resources";
const SERVICE_LIST_COLUMNS = 2;
const RESOURCE_LIST_COLUMNS = 2;
let reference: AuthoringReference | null = null;

export async function openComponentsView(): Promise<void> {
  setView("components");
  await loadReference(COMPONENTS_SERVICES, COMPONENTS_RESOURCES, componentFilter());
}

/** The collapsed reference panel on the workflow editor page. */
export async function refreshFragmentReference(): Promise<void> {
  await loadReference(FRAGMENT_SERVICES, FRAGMENT_RESOURCES, "");
}

export function filterComponentsView(): void {
  if (reference === null) return;
  renderReference(COMPONENTS_SERVICES, COMPONENTS_RESOURCES, componentFilter());
}

function componentFilter(): string {
  return input("component-search").value.trim().toLowerCase();
}

async function loadReference(
  servicesId: string,
  resourcesId: string,
  filter: string,
): Promise<void> {
  try {
    reference = await client.authoringReference();
    renderReference(servicesId, resourcesId, filter);
  } catch (error) {
    emptyTable(element(servicesId), SERVICE_LIST_COLUMNS, safeError(error));
    emptyTable(element(resourcesId), RESOURCE_LIST_COLUMNS, safeError(error));
  }
}

function renderReference(servicesId: string, resourcesId: string, filter: string): void {
  if (reference === null) return;
  const services = element(servicesId);
  clear(services);
  const resources = element(resourcesId);
  clear(resources);
  if (reference.availability === "unavailable") {
    const reason = reference.unavailableReason ?? "Authoring reference is unavailable.";
    emptyTable(services, SERVICE_LIST_COLUMNS, reason);
    emptyTable(resources, RESOURCE_LIST_COLUMNS, reason);
    return;
  }
  const visibleComponents = reference.components.filter((component) =>
    `${component.name}@${component.version}`.toLowerCase().includes(filter),
  );
  const visibleServices = reference.services.filter((service) =>
    service.name.toLowerCase().includes(filter),
  );
  if (visibleComponents.length === 0 && visibleServices.length === 0) {
    emptyTable(
      services,
      SERVICE_LIST_COLUMNS,
      filter.length > 0 ? "No services match this filter." : "No approved services are configured.",
    );
  }
  for (const component of visibleComponents) {
    const row = document.createElement("tr");
    appendCell(row, `${component.name}@${component.version}`, "cell-primary");
    appendNodeCell(row, renderComponent(component));
    services.appendChild(row);
  }
  for (const service of visibleServices) {
    const row = document.createElement("tr");
    appendCell(row, service.name, "cell-primary");
    appendNodeCell(row, renderServiceActions(service));
    services.appendChild(row);
  }

  const visibleResources = reference.resources.filter((resource) =>
    resource.name.toLowerCase().includes(filter),
  );
  const visibleTriggerBindings = reference.triggerBindings.filter((binding) =>
    binding.name.toLowerCase().includes(filter),
  );
  if (visibleResources.length === 0 && visibleTriggerBindings.length === 0) {
    emptyTable(
      resources,
      RESOURCE_LIST_COLUMNS,
      filter.length > 0
        ? "No resources match this filter."
        : "No approved resources are configured.",
    );
  }
  for (const resource of visibleResources) {
    const row = document.createElement("tr");
    appendCell(row, resource.name, "cell-primary");
    const chips = document.createElement("span");
    chips.className = "capability-chips";
    if (resource.capabilities.length === 0) {
      chips.appendChild(textNode("span", "capabilities unavailable", "capability-chip is-empty"));
    }
    for (const capability of resource.capabilities) {
      chips.appendChild(textNode("span", capability, "capability-chip"));
    }
    appendNodeCell(row, chips);
    resources.appendChild(row);
  }
  for (const binding of visibleTriggerBindings) {
    const row = document.createElement("tr");
    appendCell(row, binding.name, "cell-primary");
    appendNodeCell(row, textNode("span", `trigger · ${binding.kind}`, "capability-chip"));
    resources.appendChild(row);
  }

  if (servicesId === COMPONENTS_SERVICES) {
    const truncation = element("component-truncation-note");
    const notes = [];
    if (reference.authority === "observed_host") {
      notes.push(
        "Local entries are a safe observation of loaded host declarations, not authoritative component contracts.",
      );
    } else if (reference.catalogRevision !== null) {
      notes.push(`Immutable catalog ${reference.catalogRevision.slice(0, 12)}…`);
    }
    if (reference.truncated)
      notes.push("Some details were omitted because the reference reached its size limit.");
    truncation.hidden = notes.length === 0;
    truncation.textContent = notes.join(" ");
  }
}

function renderComponent(component: AuthoringReferenceComponent): Node {
  const container = document.createElement("div");
  if (component.description !== null) {
    container.appendChild(textNode("p", component.description));
  }
  const labels = [component.kind, ...component.capabilities];
  if (component.action !== null) labels.push(component.action);
  for (const label of labels) {
    container.appendChild(textNode("span", label, "capability-chip"));
  }
  const contracts = [
    contractText("parameters", component.parameterSchema, component.parameterContractIdentity),
    contractText("input", component.inputSchema, component.inputContractIdentity),
    contractText("output", component.outputSchema, component.outputContractIdentity),
  ].filter((contract): contract is string => contract !== null);
  if (component.resourceSlots.length > 0) {
    contracts.push(
      `resource slots: ${component.resourceSlots
        .map((slot) => `${slot.name} (${slot.capability})`)
        .join(", ")}`,
    );
  }
  if (contracts.length === 0) return container;
  const details = document.createElement("details");
  const summary = document.createElement("summary");
  summary.textContent = "authoritative contracts";
  const pre = document.createElement("pre");
  pre.className = "declaration-view";
  pre.textContent = contracts.join("\n");
  details.append(summary, pre);
  container.appendChild(details);
  return container;
}

function contractText(
  label: string,
  schema: object | null,
  identity: string | null,
): string | null {
  if (schema !== null) return `${label}: ${JSON.stringify(schema, null, 2)}`;
  if (identity !== null) return `${label} contract: ${identity}`;
  return null;
}

function renderServiceActions(service: AuthoringReferenceService): Node {
  if (service.actions.length === 0) {
    return textNode("span", "none referenced yet", "graph-node-kind");
  }
  const container = document.createElement("div");
  if (service.description !== null) {
    container.appendChild(textNode("p", service.description));
  }
  for (const action of service.actions) {
    const entry = document.createElement("div");
    entry.className = "action-entry";
    entry.appendChild(textNode("code", action.name));
    if (action.contractConflict) {
      entry.appendChild(
        textNode("span", "conflicting declared contracts", "capability-chip is-empty"),
      );
    }
    const contracts: string[] = [];
    if (action.inputSchema !== null) {
      contracts.push(`input: ${JSON.stringify(action.inputSchema, null, 2)}`);
    } else if (action.inputContractIdentity !== null) {
      contracts.push(`input contract: ${action.inputContractIdentity}`);
    }
    if (action.outputSchema !== null) {
      contracts.push(`output: ${JSON.stringify(action.outputSchema, null, 2)}`);
    } else if (action.outputContractIdentity !== null) {
      contracts.push(`output contract: ${action.outputContractIdentity}`);
    }
    if (contracts.length > 0) {
      const details = document.createElement("details");
      const summary = document.createElement("summary");
      summary.textContent = "observed declaration contracts";
      const pre = document.createElement("pre");
      pre.className = "declaration-view";
      pre.textContent = contracts.join("\n");
      details.append(summary, pre);
      entry.appendChild(details);
    }
    container.appendChild(entry);
  }
  return container;
}
