#!/usr/bin/env node

import { execSync } from "child_process";
import fs from "fs";
import path from "path";

const DUCKDB_BINDING_PATH = path.join(
  process.cwd(),
  "node_modules/duckdb/lib/binding/duckdb.node"
);

function printInfo(message) {
  console.log(`ℹ ${message}`);
}

function printSuccess(message) {
  console.log(`✓ ${message}`);
}

function printError(message) {
  console.error(`✗ ${message}`);
}

function checkDuckDBBindings() {
  return fs.existsSync(DUCKDB_BINDING_PATH);
}

function installDuckDBBindings() {
  const duckdbPath = path.join(process.cwd(), "node_modules/duckdb");
  
  if (!fs.existsSync(duckdbPath)) {
    printError("DuckDB package not found. Run 'bun install' first.");
    process.exit(1);
  }

  printInfo("Installing DuckDB native bindings...");
  
  try {
    // Run DuckDB's install script
    execSync("npm run install", {
      cwd: duckdbPath,
      stdio: "inherit",
    });
    
    if (checkDuckDBBindings()) {
      printSuccess("DuckDB native bindings installed successfully");
      return true;
    } else {
      printError("DuckDB native bindings installation failed");
      return false;
    }
  } catch (error) {
    printError(`Failed to install DuckDB bindings: ${error.message}`);
    return false;
  }
}

// Main execution
if (checkDuckDBBindings()) {
  printSuccess("DuckDB native bindings already installed");
  process.exit(0);
} else {
  printInfo("DuckDB native bindings not found, installing...");
  const success = installDuckDBBindings();
  process.exit(success ? 0 : 1);
}
