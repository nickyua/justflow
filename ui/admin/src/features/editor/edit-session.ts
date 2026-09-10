interface EditRequest {
  readonly generation: number;
  readonly revision: number;
}

/** Coordinates server responses with text that remains editable during requests. */
export class EditSession {
  #generation = 0;
  #revision = 0;
  #savedRevision = 0;
  #saving = false;

  get dirty(): boolean {
    return this.#revision !== this.#savedRevision;
  }

  edit(): void {
    this.#revision += 1;
  }

  beginLoad(): EditRequest | null {
    if (this.#saving) return null;
    this.#generation += 1;
    return { generation: this.#generation, revision: this.#revision };
  }

  acceptLoad(request: EditRequest): boolean {
    if (!this.current(request) || request.revision !== this.#revision) return false;
    this.#savedRevision = this.#revision;
    return true;
  }

  beginSave(): EditRequest | null {
    const request = this.beginLoad();
    if (request !== null) this.#saving = true;
    return request;
  }

  acceptSave(request: EditRequest): boolean {
    if (!this.current(request)) return false;
    this.#savedRevision = request.revision;
    return true;
  }

  finishSave(): void {
    this.#saving = false;
  }

  current(request: EditRequest): boolean {
    return request.generation === this.#generation;
  }
}
