import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The protobuf-es bindings are imported from ../gen/ts (outside web/), so
// bare "@bufbuild/protobuf" imports inside them do not resolve through
// web/node_modules by parent-directory walk. Alias the two entry points
// (mirroring the tsconfig paths) to the installed package.
const protobuf = (sub: string) =>
  new URL(`./node_modules/@bufbuild/protobuf/dist/esm/${sub}`, import.meta.url).pathname;

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: [
      { find: /^@bufbuild\/protobuf$/, replacement: protobuf("index.js") },
      { find: /^@bufbuild\/protobuf\/codegenv2$/, replacement: protobuf("codegenv2/index.js") },
    ],
  },
  build: {
    outDir: "dist",
  },
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:6381",
        changeOrigin: true,
      },
      "/ws": {
        target: "http://127.0.0.1:6381",
        changeOrigin: true,
        ws: true,
      },
    },
  },
});
