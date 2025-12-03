import fs from "fs";
import os from "os";
import path from "path";
export const API_PREFIX = "/api/v0";
export const README_NAME = "README.md";
export const PUBLISHER_CONFIG_NAME = "publisher.config.json";
export const PACKAGE_MANIFEST_NAME = "publisher.json";
export const MODEL_FILE_SUFFIX = ".malloy";
export const NOTEBOOK_FILE_SUFFIX = ".malloynb";
export const ROW_LIMIT = 1000;
export const TEMP_DIR_PATH = os.tmpdir();

export let publisherPath: string;

if (process.platform === "win32") {
   publisherPath = path.join(
      process.env.PROGRAMDATA || "C:\\ProgramData",
      "publisher",
   );
} else {
   publisherPath = "/etc/publisher";
}

try {
   fs.accessSync(publisherPath, fs.constants.W_OK);
} catch {
   publisherPath = path.join(os.tmpdir(), "publisher");
}

export const PUBLISHER_DATA_DIR = "publisher_data";
