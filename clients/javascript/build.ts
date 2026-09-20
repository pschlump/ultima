// Build the JavaScript distribution (design doc §11.3): bundle
// @ultima/client (clients/typescript/src, plus its @bufbuild/protobuf and
// gen/ts dependencies) into a single-file ESM build and a single-file CJS
// build, then emit type declarations with tsc.
//
// Run: bun build.ts   (from clients/javascript; `bun run build`)
import { $, type BunPlugin } from "bun";
import { join } from "node:path";

const root = import.meta.dir;
const entry = join(root, "..", "typescript", "src", "index.ts");

// gen/ts sits outside any tsconfig's tree, so the bundler's tsconfig paths
// never apply to it — alias the protobuf imports to the installed package
// directly.
const protobufAlias: BunPlugin = {
  name: "protobuf-alias",
  setup(build) {
    const base = join(root, "node_modules", "@bufbuild", "protobuf", "dist", "esm");
    build.onResolve({ filter: /^@bufbuild\/protobuf($|\/)/ }, (args) => {
      const sub = args.path.slice("@bufbuild/protobuf".length).replace(/^\//, "");
      return { path: join(base, sub === "" ? "index.js" : `${sub}/index.js`) };
    });
  },
};

const builds: { format: "esm" | "cjs"; target: "browser" | "node"; outfile: string }[] = [
  { format: "esm", target: "browser", outfile: join(root, "dist", "esm", "index.js") },
  { format: "cjs", target: "node", outfile: join(root, "dist", "cjs", "index.cjs") },
];

for (const b of builds) {
  const result = await Bun.build({
    entrypoints: [entry],
    format: b.format,
    target: b.target,
    plugins: [protobufAlias],
  });
  if (!result.success) {
    for (const log of result.logs) console.error(log);
    process.exit(1);
  }
  const output = result.outputs[0];
  if (!output) {
    console.error(`bun build produced no output for ${b.format}`);
    process.exit(1);
  }
  await Bun.write(b.outfile, output);
  console.log(`built ${b.outfile}`);
}

// Type declarations: tsc --emitDeclarationOnly over the same entry (react.ts
// is deliberately excluded — the JS distribution ships the core client only).
await $`${join(root, "node_modules", ".bin", "tsc")} -p ${join(root, "tsconfig.dts.json")}`.cwd(root);
console.log(`emitted declarations under ${join(root, "dist", "types")}`);
