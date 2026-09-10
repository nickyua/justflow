/**
 * The lazy CodeMirror composition (decision R4): YAML syntax, line numbers,
 * search, folding, undo/redo, read-only mode. No language servers, remote
 * schema retrieval, or extension registry.
 */

import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { yaml } from "@codemirror/lang-yaml";
import {
  bracketMatching,
  defaultHighlightStyle,
  foldGutter,
  foldKeymap,
  indentOnInput,
  syntaxHighlighting,
} from "@codemirror/language";
import { highlightSelectionMatches, search, searchKeymap } from "@codemirror/search";
import { Compartment, EditorState } from "@codemirror/state";
import { EditorView, keymap, lineNumbers } from "@codemirror/view";

import type { CodeEditorControl } from "./code-editor";

export function mountCodeMirror(host: HTMLTextAreaElement, onInput: () => void): CodeEditorControl {
  const readOnly = new Compartment();
  let programmatic = false;
  const view = new EditorView({
    state: EditorState.create({
      doc: host.value,
      extensions: [
        lineNumbers(),
        history(),
        foldGutter(),
        indentOnInput(),
        bracketMatching(),
        syntaxHighlighting(defaultHighlightStyle),
        highlightSelectionMatches(),
        search({ top: true }),
        yaml(),
        keymap.of([...defaultKeymap, ...historyKeymap, ...searchKeymap, ...foldKeymap]),
        readOnly.of(EditorState.readOnly.of(host.readOnly)),
        EditorView.updateListener.of((update) => {
          if (update.docChanged && !programmatic) onInput();
        }),
      ],
    }),
  });
  view.dom.classList.add("code-editor");
  view.dom.setAttribute("aria-label", host.getAttribute("aria-label") ?? "Editor");
  host.insertAdjacentElement("afterend", view.dom);
  host.hidden = true;
  return {
    value: () => view.state.doc.toString(),
    setValue: (text) => {
      programmatic = true;
      try {
        view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: text } });
      } finally {
        programmatic = false;
      }
    },
    setReadOnly: (value) => {
      view.dispatch({ effects: readOnly.reconfigure(EditorState.readOnly.of(value)) });
    },
    focus: () => view.focus(),
  };
}
