import { readFileSync } from "node:fs";

import React from "react";
import * as ReactDOM from "react-dom";
import "react-dom/client";
import TestRenderer from "react-test-renderer";

const packageJSON = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
const versions = {
  "package react": packageJSON.dependencies.react,
  "package react-dom": packageJSON.dependencies["react-dom"],
  "package react-test-renderer":
    packageJSON.devDependencies["react-test-renderer"],
  react: React.version,
  "react-dom": ReactDOM.version,
  "react-test-renderer": TestRenderer.version,
};

if (new Set(Object.values(versions)).size !== 1) {
  console.error("React packages must use one exact version:", versions);
  process.exit(1);
}

console.log(`React packages aligned at ${React.version}`);
