export const DEFAULT_ENV = "examples";

export const PACKAGES = {
   /**
    * Ecommerce model, notebook, and parquet data. The general-purpose package:
    * the model, notebook, and database screens read it, and the notebook specs
    * write their own throwaway fixtures into it.
    */
   storefront: "storefront",
   /** Givens, `#(authorize)` gates, and row-level access. */
   governed: "governed-analytics",
   /** A no-build HTML dashboard served from the package's `public/`. */
   dataApp: "html-data-app",
} as const;

/**
 * Disposable name with a timestamp suffix so parallel/repeat test runs do not
 * collide on fixtures they create and later delete.
 */
export function tmpName(prefix: string): string {
   return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}
