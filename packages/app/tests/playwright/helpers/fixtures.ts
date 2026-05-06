export const DEFAULT_ENV = "malloy-samples";

export const PACKAGES = {
   imdb: "imdb",
   ecommerce: "ecommerce",
   faa: "faa",
} as const;

/**
 * Disposable name with a timestamp suffix so parallel/repeat test runs do not
 * collide on fixtures they create and later delete.
 */
export function tmpName(prefix: string): string {
   return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}
