import { defineConfig } from "oxlint";
import core from "ultracite/oxlint/core";

export default defineConfig({
  extends: [core],
  ignorePatterns: core.ignorePatterns,
  rules: {
    "class-methods-use-this": "off",
    complexity: "off",
    "consistent-return": "off",
    "default-case": "off",
    "max-classes-per-file": "off",
    "no-bitwise": "off",
    "no-redeclare": "off",
    "no-shadow": "off",
    "no-use-before-define": "off",
    "typescript/no-unsafe-type-assertion": "off",
    "unicorn/no-array-method-this-argument": "off",
  },
});
