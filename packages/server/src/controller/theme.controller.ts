// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { sanitizeTheme, Theme } from "../config";
import { BadRequestError, FrozenConfigError } from "../errors";
import { isPublisherConfigFrozen } from "../config";
import { ThemeStore } from "../service/theme_store";

type ApiTheme = components["schemas"]["Theme"];

/**
 * Handlers for the in-app Theme Editor.
 *
 *   GET  /api/v0/theme  → current saved theme (or built-in default)
 *   PUT  /api/v0/theme  → save the supplied theme
 *   DELETE /api/v0/theme → reset to boot seed / built-in default
 */
export class ThemeController {
   constructor(private themeStore: ThemeStore) {}

   getTheme = async (): Promise<ApiTheme> => {
      const theme = await this.themeStore.get();
      // `{}` means "no overrides, use SDK defaults".
      return theme ?? {};
   };

   /**
    * Persists the theme verbatim. A literal `{}` is an intentional "clear all
    * overrides": it writes an empty row, which keeps the config's `theme`
    * seed suppressed (unlike DELETE, which drops the row so the seed
    * re-applies).
    */
   putTheme = async (body: unknown): Promise<ApiTheme> => {
      if (isPublisherConfigFrozen()) {
         throw new FrozenConfigError(
            'Cannot edit theme: publisher.config.json has "frozenConfig": true.',
         );
      }
      // sanitizeTheme returns undefined for both `{}` and garbage.
      const isEmptyObject =
         typeof body === "object" &&
         body !== null &&
         !Array.isArray(body) &&
         Object.keys(body as Record<string, unknown>).length === 0;
      const sanitized = sanitizeTheme(body, "PUT /api/v0/theme");
      if (!sanitized && !isEmptyObject) {
         throw new BadRequestError(
            "Theme payload was malformed (expected an object).",
         );
      }
      const saved: Theme = await this.themeStore.set(sanitized ?? {});
      return saved;
   };

   /** Drops the persisted row so the config's `theme` seed re-applies. */
   resetTheme = async (): Promise<ApiTheme> => {
      if (isPublisherConfigFrozen()) {
         throw new FrozenConfigError(
            'Cannot reset theme: publisher.config.json has "frozenConfig": true.',
         );
      }
      const reseeded = await this.themeStore.reset();
      return reseeded ?? {};
   };
}
