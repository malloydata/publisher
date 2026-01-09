#!/usr/bin/env node

import { execSync } from "child_process";
import fs from "fs";
import https from "https";
import os from "os";
import path from "path";

// Configuration
const ADBC_VERSION = "apache-arrow-adbc-20";
const DRIVER_VERSION = "1.8.0";
const BASE_URL = `https://github.com/apache/arrow-adbc/releases/download/${ADBC_VERSION}`;

function printInfo(message) {
  console.log(`ℹ ${message}`);
}

function printSuccess(message) {
  console.log(`✓ ${message}`);
}

function printError(message) {
  console.error(`✗ ${message}`);
}

function printWarning(message) {
  console.warn(`⚠ ${message}`);
}

async function downloadFile(url, destPath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destPath);
    https
      .get(url, (response) => {
        if (response.statusCode === 302 || response.statusCode === 301) {
          // Follow redirect
          return downloadFile(response.headers.location, destPath)
            .then(resolve)
            .catch(reject);
        }
        if (response.statusCode !== 200) {
          reject(new Error(`Failed to download: ${response.statusCode}`));
          return;
        }

        const contentLength = parseInt(response.headers["content-length"], 10);
        let downloadedBytes = 0;

        response.on("data", (chunk) => {
          downloadedBytes += chunk.length;
        });

        response.pipe(file);
        file.on("finish", () => {
          file.close();
          // Verify file size if content-length header was provided
          if (contentLength) {
            const stats = fs.statSync(destPath);
            if (stats.size !== contentLength) {
              fs.unlinkSync(destPath);
              reject(
                new Error(
                  `Download incomplete: expected ${contentLength} bytes, got ${stats.size}`,
                ),
              );
              return;
            }
          }
          resolve();
        });
      })
      .on("error", (err) => {
        if (fs.existsSync(destPath)) {
          fs.unlinkSync(destPath);
        }
        reject(err);
      });
  });
}

function isValidZipFile(filePath) {
  try {
    // Check if file exists and has content
    if (!fs.existsSync(filePath)) {
      return false;
    }

    const stats = fs.statSync(filePath);
    if (stats.size === 0) {
      return false;
    }

    // Read first few bytes to check ZIP signature
    const fd = fs.openSync(filePath, "r");
    const buffer = Buffer.alloc(4);
    fs.readSync(fd, buffer, 0, 4, 0);
    fs.closeSync(fd);

    // ZIP files start with PK (0x50 0x4B) - "PK" signature
    // Wheel files are ZIP archives
    return buffer[0] === 0x50 && buffer[1] === 0x4b;
  } catch (error) {
    return false;
  }
}

function detectPlatform() {
  const platform = os.platform();
  const arch = os.arch();

  if (platform === "win32") {
    return {
      os: "windows",
      arch: arch === "x64" ? "amd64" : arch,
      wheelName: `adbc_driver_snowflake-${DRIVER_VERSION}-py3-none-win_amd64.whl`,
      driverFile: "adbc_driver_snowflake.dll",
    };
  } else if (platform === "darwin") {
    // macOS wheel names match the bash script exactly
    if (arch === "arm64") {
      return {
        os: "osx",
        arch: "arm64",
        wheelName: `adbc_driver_snowflake-${DRIVER_VERSION}-py3-none-macosx_11_0_arm64.whl`,
        driverFile: "libadbc_driver_snowflake.dylib",
      };
    } else {
      // x86_64 macOS
      return {
        os: "osx",
        arch: "amd64",
        wheelName: `adbc_driver_snowflake-${DRIVER_VERSION}-py3-none-macosx_10_15_x86_64.whl`,
        driverFile: "libadbc_driver_snowflake.dylib",
      };
    }
  } else {
    // Linux - wheel names have multiple platform tags
    if (arch === "arm64" || arch === "aarch64") {
      return {
        os: "linux",
        arch: "aarch64",
        wheelName: `adbc_driver_snowflake-${DRIVER_VERSION}-py3-none-manylinux2014_aarch64.manylinux_2_17_aarch64.whl`,
        driverFile: "libadbc_driver_snowflake.so",
      };
    } else {
      // x86_64 Linux
      return {
        os: "linux",
        arch: "amd64",
        wheelName: `adbc_driver_snowflake-${DRIVER_VERSION}-py3-none-manylinux1_x86_64.manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_5_x86_64.whl`,
        driverFile: "libadbc_driver_snowflake.so",
      };
    }
  }
}

async function installDuckDB() {
  const platform = os.platform();

  if (platform === "win32") {
    printWarning("DuckDB CLI installation script is not supported on Windows");
    printInfo(
      "Please install DuckDB manually from https://duckdb.org/docs/installation/",
    );
    printInfo(
      "Or use DuckDB via npm package (already installed as dependency)",
    );
    return;
  }

  printInfo("Installing DuckDB CLI...");
  try {
    execSync("curl -L https://install.duckdb.org | bash", {
      stdio: "inherit",
      shell: "/bin/bash",
    });
    const homeDir = os.homedir();
    process.env.PATH = `${homeDir}/.duckdb/cli/latest:${process.env.PATH}`;
    printSuccess("DuckDB CLI installed successfully");
  } catch (error) {
    printWarning(`DuckDB CLI installation failed: ${error.message}`);
    printInfo("Continuing with ADBC driver installation...");
  }
}

async function installADBCDriver() {
  const platformInfo = detectPlatform();
  const homeDir = os.homedir();

  // Determine install directory
  let installDir;
  if (platformInfo.os === "windows") {
    installDir = path.join(homeDir, ".duckdb", "adbc_drivers");
  } else {
    installDir = path.join(homeDir, ".duckdb", "adbc_drivers");
  }

  // Create install directory if it doesn't exist
  if (!fs.existsSync(installDir)) {
    fs.mkdirSync(installDir, { recursive: true });
  }

  const wheelPath = path.join(installDir, platformInfo.wheelName);
  const wheelUrl = `${BASE_URL}/${platformInfo.wheelName}`;

  // Check if wheel exists and is valid
  const needsDownload = !fs.existsSync(wheelPath) || !isValidZipFile(wheelPath);

  if (needsDownload) {
    if (fs.existsSync(wheelPath)) {
      printWarning("Existing wheel file appears corrupted, re-downloading...");
      fs.unlinkSync(wheelPath);
    }

    printInfo(`Downloading ADBC Snowflake driver for ${platformInfo.os}...`);
    printInfo(`URL: ${wheelUrl}`);

    try {
      await downloadFile(wheelUrl, wheelPath);
      printSuccess(`Downloaded ${platformInfo.wheelName}`);
    } catch (error) {
      printError(`Failed to download ADBC driver: ${error.message}`);
      process.exit(1);
    }
  } else {
    printInfo(
      "Driver wheel already exists and appears valid, checking if extraction is needed...",
    );
  }

  // Extract driver from wheel
  printInfo("Extracting driver library...");

  try {
    const extractedDir = path.join(installDir, "adbc_driver_snowflake");
    const finalPath = path.join(installDir, platformInfo.driverFile);

    if (platformInfo.os === "windows") {
      // On Windows, PowerShell Expand-Archive doesn't recognize .whl files
      // Rename to .zip temporarily for extraction
      const zipPath = wheelPath.replace(/\.whl$/, ".zip");
      fs.copyFileSync(wheelPath, zipPath);

      try {
        const zipPathEscaped = zipPath.replace(/'/g, "''");
        const installDirEscaped = installDir.replace(/'/g, "''");
        const extractCmd = `powershell -Command "Expand-Archive -Path '${zipPathEscaped}' -DestinationPath '${installDirEscaped}' -Force"`;
        execSync(extractCmd, { stdio: "inherit", shell: true });
      } finally {
        // Clean up temporary zip file
        if (fs.existsSync(zipPath)) {
          fs.unlinkSync(zipPath);
        }
      }
    } else {
      // On Unix systems, use unzip (which handles .whl files directly)
      execSync(
        `unzip -o "${wheelPath}" "adbc_driver_snowflake/*" -d "${installDir}"`,
        {
          stdio: "inherit",
          shell: true,
        },
      );
    }

    // Look for driver file (try multiple possible names)
    const possibleDriverFiles = [
      platformInfo.driverFile,
      "libadbc_driver_snowflake.so", // Some wheels use .so even on Windows
      "adbc_driver_snowflake.dll",
    ];

    let foundDriverFile = null;
    for (const driverFile of possibleDriverFiles) {
      const extractedPath = path.join(extractedDir, driverFile);
      if (fs.existsSync(extractedPath)) {
        foundDriverFile = driverFile;
        break;
      }
    }

    if (!foundDriverFile) {
      printError("Driver library not found in wheel");
      printInfo(`Searched for: ${possibleDriverFiles.join(", ")}`);
      printInfo(`In directory: ${extractedDir}`);
      process.exit(1);
    }

    // Move driver to install directory
    const extractedPath = path.join(extractedDir, foundDriverFile);
    if (fs.existsSync(finalPath)) {
      fs.unlinkSync(finalPath);
    }
    fs.renameSync(extractedPath, finalPath);

    // Clean up extracted directory
    if (fs.existsSync(extractedDir)) {
      fs.rmSync(extractedDir, { recursive: true, force: true });
    }

    printSuccess(`Extracted ${foundDriverFile} -> ${platformInfo.driverFile}`);
  } catch (error) {
    printError(`Failed to extract driver: ${error.message}`);
    process.exit(1);
  }

  // Verify installation
  const finalPath = path.join(installDir, platformInfo.driverFile);
  if (fs.existsSync(finalPath)) {
    const stats = fs.statSync(finalPath);
    const fileSize = (stats.size / 1024 / 1024).toFixed(2);

    printSuccess("ADBC Snowflake driver installed successfully!");
    console.log("");
    console.log("Installation details:");
    console.log(`  • Driver: ${platformInfo.driverFile}`);
    console.log(`  • Location: ${finalPath}`);
    console.log(`  • Size: ${fileSize} MB`);
    console.log(`  • Version: ${DRIVER_VERSION}`);
    console.log("");

    if (platformInfo.os === "windows") {
      console.log("To use with DuckDB Snowflake extension:");
      console.log(`  1. The extension will automatically find the driver at:`);
      console.log(`     ${finalPath}`);
      console.log(`  2. If you have issues, set the environment variable:`);
      console.log(`     set SNOWFLAKE_ADBC_DRIVER_PATH=${finalPath}`);
    } else {
      console.log("To use with DuckDB Snowflake extension:");
      console.log(`  1. The extension will automatically find the driver at:`);
      console.log(`     ${finalPath}`);
      console.log(`  2. If you have issues, set the environment variable:`);
      console.log(`     export SNOWFLAKE_ADBC_DRIVER_PATH="${finalPath}"`);
    }

    console.log("");
    printSuccess(
      "Installation complete! You can now use the Snowflake extension.",
    );
  } else {
    printError("Installation verification failed");
    process.exit(1);
  }
}

async function main() {
  console.log("");
  console.log(
    "════════════════════════════════════════════════════════════════",
  );
  console.log("     DuckDB Snowflake ADBC Driver Installer");
  console.log(`     Version: ${DRIVER_VERSION}`);
  console.log(
    "════════════════════════════════════════════════════════════════",
  );
  console.log("");

  try {
    await installDuckDB();
    await installADBCDriver();
  } catch (error) {
    printError(`Installation failed: ${error.message}`);
    process.exit(1);
  }
}

// Run main function
main();
