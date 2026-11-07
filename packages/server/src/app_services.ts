// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { CompileController } from "./controller/compile.controller";
import type { ConnectionController } from "./controller/connection.controller";
import type { DatabaseController } from "./controller/database.controller";
import type { MaterializationController } from "./controller/materialization.controller";
import type { ModelController } from "./controller/model.controller";
import type { PackageController } from "./controller/package.controller";
import type { QueryController } from "./controller/query.controller";
import type { ReportController } from "./controller/report.controller";
import type { SkillController } from "./controller/skill.controller";
import type { StorageDestinationController } from "./controller/storage_destination.controller";
import type { ThemeController } from "./controller/theme.controller";
import type { VersionController } from "./controller/version.controller";
import type { WatchModeController } from "./controller/watch-mode.controller";
import type { EnvironmentStore } from "./service/environment_store";
import type { ThemeStore } from "./service/theme_store";

/**
 * The application services the composition root (main_server.ts) builds once
 * and hands to every adapter: the REST routes take the whole record, the MCP
 * server the slice in {@link McpServices}. An adapter calls these instances; it
 * never constructs its own, so REST and MCP share one controller (and its
 * state, such as PackageController's warn-once sets) per process.
 */
export interface AppServices {
   environmentStore: EnvironmentStore;
   themeStore: ThemeStore;
   themeController: ThemeController;
   watchModeController: WatchModeController;
   connectionController: ConnectionController;
   packageController: PackageController;
   modelController: ModelController;
   skillController: SkillController;
   reportController: ReportController;
   databaseController: DatabaseController;
   queryController: QueryController;
   compileController: CompileController;
   materializationController: MaterializationController;
   storageDestinationController: StorageDestinationController;
   versionController: VersionController;
}

/** The services the MCP server's tools call. */
export type McpServices = Pick<
   AppServices,
   | "environmentStore"
   | "compileController"
   | "packageController"
   | "connectionController"
>;
