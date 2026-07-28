export const DEFAULT_ENV = "examples";

export const PACKAGES = {
   /**
    * Ecommerce model, notebook, dashboards, a no-build HTML data app, and
    * parquet data. The general-purpose package: the model, notebook, dashboard,
    * data-app, and database screens all read it, and the notebook specs write
    * their own throwaway fixtures into it.
    */
   storefront: "storefront",
   /** Givens, `#(authorize)` gates, and row-level access. */
   governed: "governed-analytics",
} as const;

/**
 * Disposable name with a timestamp suffix so parallel/repeat test runs do not
 * collide on fixtures they create and later delete.
 */
export function tmpName(prefix: string): string {
   return `${prefix}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}
