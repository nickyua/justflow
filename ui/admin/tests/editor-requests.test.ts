import { beforeEach, expect, it, vi } from "vitest";
import type { DraftRecord } from "../src/api/contracts";

const boundary = vi.hoisted(() => ({
  client: {
    draft: vi.fn(),
    draftYaml: vi.fn(),
    configurationRelationships: vi.fn(),
    saveYamlDraft: vi.fn(),
    triggersFragment: vi.fn(),
    saveTriggersFragment: vi.fn(),
  },
  surfaces: new Map<string, EventTarget & { value: string }>(),
  status: vi.fn(),
}));

vi.mock("../src/app/api", () => ({ client: boundary.client, requestHeaders: vi.fn() }));
vi.mock("../src/app/capabilities", () => ({ getCapabilities: () => null }));
vi.mock("../src/app/dom", () => ({
  element: () => ({ hidden: false }),
  clear: vi.fn(),
  emptyTable: vi.fn(),
  textarea: (id: string) => {
    let host = boundary.surfaces.get(id);
    if (host === undefined) {
      host = Object.assign(new EventTarget(), { value: "" });
      boundary.surfaces.set(id, host);
    }
    return host;
  },
}));
vi.mock("../src/components/status", () => ({ showStatus: boundary.status }));
vi.mock("../src/app/refresh", () => ({ refreshDashboard: vi.fn() }));
vi.mock("../src/app/router", () => ({ setView: vi.fn() }));
vi.mock("../src/features/releases", () => ({}));

const INITIAL_VERSION = 7;
const SAVED_VERSION = INITIAL_VERSION + 1;
const INITIAL_TEXT = "workflows: {}\n";
const SUBMITTED_TEXT = "workflows: {}\n# submitted\n";
const NEWER_TEXT = "workflows: {}\n# still editing\n";

function record(version = INITIAL_VERSION): DraftRecord {
  return { version, bundle: { workflows: {}, triggers: {} }, restartRequired: false };
}

function relationships(version = INITIAL_VERSION) {
  return {
    workingVersion: version,
    activeIdentity: null,
    relationships: [],
    restartRequired: false,
  };
}

function deferred<T>() {
  return Promise.withResolvers<T>();
}

function surface(id: string) {
  const host = boundary.surfaces.get(id);
  if (host === undefined) throw new Error(`Missing editor surface ${id}`);
  return host;
}

function type(id: string, text: string): void {
  const host = surface(id);
  host.value = text;
  host.dispatchEvent(new Event("input"));
}

beforeEach(() => {
  vi.resetModules();
  vi.resetAllMocks();
  boundary.surfaces.clear();
  vi.stubGlobal("window", { confirm: () => true });
  boundary.client.draft.mockResolvedValue(record());
  boundary.client.draftYaml.mockResolvedValue(INITIAL_TEXT);
  boundary.client.configurationRelationships.mockResolvedValue(relationships());
  boundary.client.triggersFragment.mockResolvedValue({
    version: INITIAL_VERSION,
    document: INITIAL_TEXT,
  });
});

it.each(["document", "triggers"] as const)(
  "keeps edits typed during a %s save and blocks overlapping saves",
  async (kind) => {
    const editor = await import("../src/features/editor");
    const load = kind === "document" ? editor.loadDraft : editor.loadSchedulesFragment;
    const save = kind === "document" ? editor.saveDraft : editor.saveSchedulesFragment;
    const saveRequest =
      kind === "document" ? boundary.client.saveYamlDraft : boundary.client.saveTriggersFragment;
    const id = kind === "document" ? "configuration-editor" : "schedules-editor";
    await load();
    type(id, SUBMITTED_TEXT);
    const pending = deferred<DraftRecord>();
    saveRequest.mockReturnValueOnce(pending.promise);
    const saving = save();
    type(id, NEWER_TEXT);
    await save();
    expect(saveRequest).toHaveBeenCalledTimes(1);
    boundary.client.configurationRelationships.mockResolvedValue(relationships(SAVED_VERSION));
    pending.resolve(record(SAVED_VERSION));
    await saving;
    expect(surface(id).value).toBe(NEWER_TEXT);
    expect(editor.isEditorDirty()).toBe(true);
    expect(saveRequest.mock.calls[0]?.[0]).toBe(SUBMITTED_TEXT);
  },
);

it("keeps typing that occurs while a document reload is pending", async () => {
  const editor = await import("../src/features/editor");
  await editor.loadDraft();
  const pending = deferred<string>();
  boundary.client.draftYaml.mockReturnValueOnce(pending.promise);
  const loading = editor.loadDraft();
  await vi.waitFor(() => expect(boundary.client.draftYaml).toHaveBeenCalledTimes(2));
  type("configuration-editor", NEWER_TEXT);
  pending.resolve("obsolete response");
  await loading;
  expect(surface("configuration-editor").value).toBe(NEWER_TEXT);
  expect(editor.isEditorDirty()).toBe(true);
  expect(boundary.client.draftYaml).toHaveBeenLastCalledWith(INITIAL_VERSION);
});

it("does not advance the full document CAS token when a trigger fragment is saved", async () => {
  const editor = await import("../src/features/editor");
  await editor.loadDraft();
  await editor.loadSchedulesFragment();
  type("schedules-editor", SUBMITTED_TEXT);
  boundary.client.saveTriggersFragment.mockResolvedValue(record(SAVED_VERSION));
  boundary.client.configurationRelationships.mockResolvedValue(relationships(SAVED_VERSION));
  await editor.saveSchedulesFragment();
  type("configuration-editor", NEWER_TEXT);
  boundary.client.saveYamlDraft.mockRejectedValue(new Error("Version conflict"));
  await editor.saveDraft();
  expect(boundary.client.saveYamlDraft).toHaveBeenCalledWith(NEWER_TEXT, record());
  expect(editor.isEditorDirty()).toBe(true);
  expect(surface("configuration-editor").value).toBe(NEWER_TEXT);
});
