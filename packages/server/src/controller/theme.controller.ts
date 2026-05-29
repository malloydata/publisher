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
   constructor(
      private themeStore: ThemeStore,
      private serverRoot: string,
   ) {}

   getTheme = async (): Promise<ApiTheme> => {
      const theme = await this.themeStore.get();
      // Always return an object so the client never has to deal with the
      // null/undefined ambiguity. An empty object means "no overrides yet,
      // use SDK defaults."
      return theme ?? {};
   };

   putTheme = async (body: unknown): Promise<ApiTheme> => {
      if (isPublisherConfigFrozen(this.serverRoot)) {
         throw new FrozenConfigError(
            'Cannot edit theme: publisher.config.json has "frozenConfig": true.',
         );
      }
      // Treat a literal `{}` as an intentional "clear all overrides" PUT
      // (the only other clear path is DELETE, which re-seeds from the
      // config file). sanitizeTheme returns undefined for both `{}` and
      // garbage, so we have to disambiguate here.
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

   resetTheme = async (): Promise<ApiTheme> => {
      if (isPublisherConfigFrozen(this.serverRoot)) {
         throw new FrozenConfigError(
            'Cannot reset theme: publisher.config.json has "frozenConfig": true.',
         );
      }
      const reseeded = await this.themeStore.reset();
      return reseeded ?? {};
   };
}
