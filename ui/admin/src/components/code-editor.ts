import { textarea } from "../app/dom";

/** One editing surface: a CodeMirror upgrade when its lazy chunk loads, the textarea otherwise. */
export interface CodeEditorControl {
  value(): string;
  setValue(text: string): void;
  setReadOnly(readOnly: boolean): void;
  focus(): void;
}

export async function upgradeTextarea(
  textareaId: string,
  onInput: () => void,
): Promise<CodeEditorControl> {
  const host = textarea(textareaId);
  try {
    const { mountCodeMirror } = await import("./codemirror-editor");
    return mountCodeMirror(host, onInput);
  } catch {
    // The editor chunk failed to load; the plain textarea remains fully functional.
    host.addEventListener("input", onInput);
    return textareaControl(host);
  }
}

/** Plain-textarea control — the pre-upgrade surface and the fallback when the
 * CodeMirror chunk cannot load. CodeMirror seeds from the host's value on mount,
 * so content set through this control survives a later upgrade. */
export function textareaControl(host: HTMLTextAreaElement): CodeEditorControl {
  return {
    value: () => host.value,
    setValue: (text) => {
      host.value = text;
    },
    setReadOnly: (readOnly) => {
      host.readOnly = readOnly;
    },
    focus: () => host.focus(),
  };
}
