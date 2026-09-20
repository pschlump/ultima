# ultima-client (JavaScript distribution)

Plain-JavaScript distribution of the TypeScript client
(`clients/typescript`, design doc §11.3): single-file ESM and CJS bundles of
`@ultima/client` with all dependencies (protobuf-es, generated `gen/ts`
bindings) inlined, plus emitted type declarations. The React hooks subpath
is not included (the core is framework-agnostic).

## Build

```
bun install
bun run build        # build.ts: bun build → dist/esm/index.js + dist/cjs/index.cjs,
                     #           tsc --emitDeclarationOnly → dist/types/
```

## Use

```js
// ESM (browser <script type="module">, bun, node):
import { UltimaClient } from "ultima-client";        // or ./dist/esm/index.js

// CJS (node require):
const { UltimaClient } = require("ultima-client");   // or ./dist/cjs/index.cjs

const client = new UltimaClient({ baseUrl: "http://127.0.0.1:6381" });
client.connect();
console.log(await client.ws.ping());
```

Package entry points: `exports.import` → `dist/esm/index.js`,
`exports.require` → `dist/cjs/index.cjs`, `exports.types` → the emitted
`index.d.ts` tree.

`smoke.html` demonstrates no-build `<script type="module">` usage against
the ESM bundle — serve this directory statically next to a running
`ultima-server` and open it.

## Smoke checks

```
bun run smoke    # bun smoke.mjs && bun smoke.cjs && node smoke.mjs && node smoke.cjs
```

Each smoke imports the built bundle and constructs a client (no network),
proving both module formats load under both runtimes.
