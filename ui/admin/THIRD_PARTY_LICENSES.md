# Bundled third-party licenses

The packaged administration panel bundles the following runtime dependencies
into its lazy `editor.js` chunk. All are MIT-licensed.

| Package | Version | License | Copyright |
|---|---|---|---|
| @codemirror/state | 6.7.1 | MIT | 2018-2021 by Marijn Haverbeke and others |
| @codemirror/view | 6.43.8 | MIT | 2018-2021 by Marijn Haverbeke and others |
| @codemirror/language | 6.12.4 | MIT | 2018-2021 by Marijn Haverbeke and others |
| @codemirror/commands | 6.10.4 | MIT | 2018-2021 by Marijn Haverbeke and others |
| @codemirror/search | 6.7.1 | MIT | 2018-2021 by Marijn Haverbeke and others |
| @codemirror/lang-yaml | 6.1.3 | MIT | 2018-2021 by Marijn Haverbeke and others |
| @lezer/* (transitive) | — | MIT | 2018 by Marijn Haverbeke and others |
| style-mod, w3c-keyname, crelt (transitive) | — | MIT | Marijn Haverbeke |

Full license texts ship in each package under `node_modules/<name>/LICENSE`
and are reproduced by the MIT terms above. This file is the bundle's license
inventory required by the editor acceptance gates; update it when the pinned
versions change.
