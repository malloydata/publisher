// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { access } from "./access";
import { admin } from "./admin";
import { launch } from "./launch";
import { mcp } from "./mcp";
import { query } from "./query";
import { retrieval } from "./retrieval";
import { server } from "./server";
import { storage } from "./storage";

/**
 * Every Publisher setting, in one list. The files hold one table of the
 * configuration design's mapping each, so the generated reference keeps the
 * same sections; this list is the registry the rest of the server sees.
 */
export const REGISTRY = [
   ...server,
   ...launch,
   ...mcp,
   ...admin,
   ...query,
   ...storage,
   ...access,
   ...retrieval,
];

export type RegisteredSetting = (typeof REGISTRY)[number];
export type SettingKey = RegisteredSetting["key"];
/** The type `settings.get(key)` returns: `settings.get("mcp.port")` is a number. */
export type SettingValue<K extends SettingKey> =
   Extract<RegisteredSetting, { key: K }> extends {
      valueType?: { value: infer T };
   }
      ? T
      : never;
