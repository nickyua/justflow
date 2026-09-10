import { createHash } from "node:crypto";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import type { Plugin } from "vite";
import { defineConfig } from "vitest/config";

const OUT_DIR = resolve(
  import.meta.dirname,
  "../../packages/justflow-admin/src/justflow_admin/assets/dist",
);

/**
 * Emits the build allowlist the Python asset server enforces: exact asset
 * paths, bytes, and digests — measured from the final on-disk files, after
 * every Vite post-processing pass.
 */
function justflowManifest(): Plugin {
  return {
    name: "justflow-manifest",
    apply: "build",
    closeBundle() {
      const files: Record<string, { bytes: number; sha256: string }> = {};
      const assetsDir = join(OUT_DIR, "assets");
      for (const entry of readdirSync(assetsDir, { withFileTypes: true })) {
        if (!entry.isFile()) continue;
        const data = readFileSync(join(assetsDir, entry.name));
        files[`assets/${entry.name}`] = {
          bytes: data.length,
          sha256: createHash("sha256").update(data).digest("hex"),
        };
      }
      writeFileSync(join(OUT_DIR, "justflow-manifest.json"), JSON.stringify({ files }, null, 1));
    },
  };
}

export default defineConfig({
  base: "/admin/",
  plugins: [justflowManifest()],
  build: {
    assetsDir: "assets",
    emptyOutDir: true,
    manifest: false,
    modulePreload: false,
    outDir: OUT_DIR,
    rollupOptions: {
      input: {
        app: resolve(import.meta.dirname, "index.html"),
        "graph-frame": resolve(import.meta.dirname, "graph-frame.html"),
      },
      output: {
        assetFileNames: "assets/[name]-[hash][extname]",
        chunkFileNames: "assets/[name]-[hash].js",
        entryFileNames: "assets/[name]-[hash].js",
        manualChunks(id: string) {
          if (id.includes("@codemirror") || id.includes("@lezer") || id.includes("@marijn")) {
            return "editor";
          }
          if (/node_modules\/(style-mod|w3c-keyname|crelt)\//.test(id)) {
            return "editor";
          }
          return undefined;
        },
      },
    },
  },
  test: {
    coverage: {
      exclude: [
        "src/main.ts",
        "src/app/**",
        "src/components/**",
        "src/features/**",
        "src/graph-frame/main.ts",
      ],
      include: ["src/**/*.ts"],
      provider: "v8",
      reporter: ["text"],
      thresholds: {
        branches: 80,
        functions: 80,
        lines: 80,
        statements: 80,
      },
    },
    environment: "node",
  },
});
