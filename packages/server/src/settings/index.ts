// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { SettingKey, SettingValue } from "./registry";
import { Settings } from "./settings";

/**
 * The one way code reads a setting: `settings.get("mcp.port")`, typed by the
 * registry, from the settings this process started with. See
 * docs/configuration-design.md.
 */
export const settings = {
   get: <K extends SettingKey>(key: K): SettingValue<K> =>
      Settings.current().get(key),
   source: (key: SettingKey) => Settings.current().source(key),
   isSet: (key: SettingKey) => Settings.current().isSet(key),
   shadowed: (key: SettingKey) => Settings.current().shadowed(key),
};

export { SettingError, SettingsError, type SettingSource } from "./errors";
export { externalEnv, referencedVariable } from "./external";
export { REGISTRY, type SettingKey, type SettingValue } from "./registry";
export { derivedEnvName, derivedFlagName } from "./setting";
export {
   Settings,
   type LoadInput,
   type SerializedSettings,
   type SettingWarning,
} from "./settings";
