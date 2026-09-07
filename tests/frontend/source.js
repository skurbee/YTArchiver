"use strict";

const fs = require("node:fs");
const path = require("node:path");
const web = path.join(__dirname, "..", "..", "web");

// VM tests use the same state owner and prerequisite utilities as the app.
// Load complete modules, preserving each test's injected native bridge/DOM.
function frontendSource(name) {
  const dependencies = name === "syncSubbed.js" ? ["eventState.js"] : [];
  return ["util.js", "browseState.js", ...dependencies, name]
    .filter((value, index, names) => names.indexOf(value) === index)
    .map(file => fs.readFileSync(path.join(web, file), "utf8"))
    .join("\n");
}

module.exports = { frontendSource };
