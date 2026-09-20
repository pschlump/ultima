// CJS smoke: the bundled dist/cjs/index.cjs loads via require() and
// constructs a client without touching the network.
// Run: node smoke.cjs / bun smoke.cjs
const {
  AuthManager,
  MemoryStorage,
  RestClient,
  UltimaClient,
  UltimaWS,
} = require("./dist/cjs/index.cjs");

const ctors = { UltimaClient, UltimaWS, RestClient, AuthManager, MemoryStorage };
for (const [name, ctor] of Object.entries(ctors)) {
  if (typeof ctor !== "function") {
    console.error(`FAIL: ${name} is not exported from the CJS bundle`);
    process.exit(1);
  }
}

const client = new UltimaClient({ baseUrl: "http://127.0.0.1:6381", storage: new MemoryStorage() });
if (!(client.ws instanceof UltimaWS) || !(client.rest instanceof RestClient)) {
  console.error("FAIL: UltimaClient did not wire ws/rest");
  process.exit(1);
}
console.log("PASS: CJS bundle loads and constructs (bun/node)");
