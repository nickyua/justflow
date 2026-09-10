import { describe, expect, it } from "vitest";
import { EditSession } from "../src/features/editor/edit-session";

describe("editor requests", () => {
  it.each([
    { id: "no edits", editDuringRequest: false, secondLoad: false, accepted: true },
    { id: "typing during load", editDuringRequest: true, secondLoad: false, accepted: false },
    { id: "newer load wins", editDuringRequest: false, secondLoad: true, accepted: false },
  ])("$id", ({ editDuringRequest, secondLoad, accepted }) => {
    const session = new EditSession();
    const request = session.beginLoad();
    if (request === null) throw new Error("initial load must be admitted");
    if (editDuringRequest) session.edit();
    if (secondLoad) session.beginLoad();
    expect(session.acceptLoad(request)).toBe(accepted);
    expect(session.dirty).toBe(editDuringRequest);
  });

  it.each([false, true])("preserves edits made during save: %s", (editDuringSave) => {
    const session = new EditSession();
    session.edit();
    const request = session.beginSave();
    if (request === null) throw new Error("initial save must be admitted");
    expect(session.beginLoad()).toBeNull();
    expect(session.beginSave()).toBeNull();
    if (editDuringSave) session.edit();
    expect(session.acceptSave(request)).toBe(true);
    session.finishSave();
    expect(session.dirty).toBe(editDuringSave);
    expect(session.beginLoad()).not.toBeNull();
  });

  it("a failed save leaves edits dirty and permits retry", () => {
    const session = new EditSession();
    session.edit();
    session.beginSave();
    session.finishSave();
    expect(session.dirty).toBe(true);
    expect(session.beginSave()).not.toBeNull();
  });
});
