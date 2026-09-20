// ESM smoke: the bundled dist/esm/index.js loads and constructs a client
// without touching the network. Run: bun smoke.mjs / node smoke.mjs
import {
  AuthManager,
  MemoryStorage,
  RestClient,
  UltimaClient,
  UltimaWS,
} from "./dist/esm/index.js";

const ctors = { UltimaClient, UltimaWS, RestClient, AuthManager, MemoryStorage };
for (const [name, ctor] of Object.entries(ctors)) {
  if (typeof ctor !== "function") {
    console.error(`FAIL: ${name} is not exported from the ESM bundle`);
    process.exit(1);
  }
}

const client = new UltimaClient({ baseUrl: "http://127.0.0.1:6381", storage: new MemoryStorage() });
if (!(client.ws instanceof UltimaWS) || !(client.rest instanceof RestClient)) {
  console.error("FAIL: UltimaClient did not wire ws/rest");
  process.exit(1);
}
if (client.ws.state !== "connecting") {
  console.error(`FAIL: fresh client state = ${client.ws.state}, want connecting (no auto-dial)`);
  process.exit(1);
}
console.log("PASS: ESM bundle loads and constructs (bun/node)");
