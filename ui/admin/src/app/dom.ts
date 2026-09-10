export function element(id: string): HTMLElement {
  const value = document.getElementById(id);
  if (value === null) throw new Error(`Administration panel element is missing: ${id}`);
  return value;
}

export function button(id: string): HTMLButtonElement {
  const value = element(id);
  if (!(value instanceof HTMLButtonElement)) throw new TypeError(`${id} must be a button`);
  return value;
}

export function input(id: string): HTMLInputElement {
  const value = element(id);
  if (!(value instanceof HTMLInputElement)) throw new TypeError(`${id} must be an input`);
  return value;
}

export function selectElement(id: string): HTMLSelectElement {
  const value = element(id);
  if (!(value instanceof HTMLSelectElement)) throw new TypeError(`${id} must be a select`);
  return value;
}

export function dialog(id: string): HTMLDialogElement {
  const value = element(id);
  if (!(value instanceof HTMLDialogElement)) throw new TypeError(`${id} must be a dialog`);
  return value;
}

export function textarea(id: string): HTMLTextAreaElement {
  const value = element(id);
  if (!(value instanceof HTMLTextAreaElement)) throw new TypeError(`${id} must be a textarea`);
  return value;
}

export function form(id: string): HTMLFormElement {
  const value = element(id);
  if (!(value instanceof HTMLFormElement)) throw new TypeError(`${id} must be a form`);
  return value;
}

export function clear(node: HTMLElement): void {
  node.replaceChildren();
}

export function textNode(tag: string, text: string, className?: string): HTMLElement {
  const node = document.createElement(tag);
  node.textContent = text;
  if (className !== undefined) node.className = className;
  return node;
}

export function actionButton(text: string, className: string): HTMLButtonElement {
  const node = document.createElement("button");
  node.textContent = text;
  node.className = className;
  node.type = "button";
  return node;
}

export function emptyTable(node: HTMLElement, columns: number, message: string): void {
  clear(node);
  const row = document.createElement("tr");
  const cell = document.createElement("td");
  cell.className = "empty-cell";
  cell.colSpan = columns;
  cell.textContent = message;
  row.appendChild(cell);
  node.appendChild(row);
}

export function appendCell(row: HTMLTableRowElement, value: string, className?: string): void {
  const cell = document.createElement("td");
  cell.textContent = value;
  if (className !== undefined) cell.className = className;
  row.appendChild(cell);
}

export function appendNodeCell(row: HTMLTableRowElement, child: Node): void {
  const cell = document.createElement("td");
  cell.appendChild(child);
  row.appendChild(cell);
}
