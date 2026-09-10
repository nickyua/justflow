import { ApiRequestError } from "../api/client";

export interface Page<Item> {
  items: Item[];
  nextCursor: string | null;
}

const STALE_CURSOR_CODE = "stale_cursor";

export type LoadMoreOutcome = "appended" | "reset" | "complete";

/** Accumulates bounded server pages behind one opaque continuation cursor. */
export class PagedCollection<Item> {
  #items: Item[] = [];
  #cursor: string | null = null;
  readonly #fetchPage: (cursor: string | null) => Promise<Page<Item>>;

  constructor(fetchPage: (cursor: string | null) => Promise<Page<Item>>) {
    this.#fetchPage = fetchPage;
  }

  get items(): readonly Item[] {
    return this.#items;
  }

  get hasMore(): boolean {
    return this.#cursor !== null;
  }

  async reload(): Promise<void> {
    const page = await this.#fetchPage(null);
    this.#items = page.items;
    this.#cursor = page.nextCursor;
  }

  /** Appends the next page; a stale cursor reloads from the first page and reports it. */
  async loadMore(): Promise<LoadMoreOutcome> {
    if (this.#cursor === null) return "complete";
    try {
      const page = await this.#fetchPage(this.#cursor);
      this.#items = [...this.#items, ...page.items];
      this.#cursor = page.nextCursor;
      return "appended";
    } catch (error) {
      if (error instanceof ApiRequestError && error.code === STALE_CURSOR_CODE) {
        await this.reload();
        return "reset";
      }
      throw error;
    }
  }
}

export const STALE_LIST_NOTICE =
  "The list changed on the server while paging; it was reloaded from the first page.";
