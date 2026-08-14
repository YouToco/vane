import assert from "node:assert/strict";

const expected = "22.23.2";
assert.equal(
  process.versions.node,
  expected,
  `Vane Web requires Node ${expected}; current runtime is ${process.versions.node}`,
);
console.log(`Node version verified: ${expected}`);
