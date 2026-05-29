// Minimal type stubs for the transitive map deps the MapPreview uses.
// Both `d3-geo` and `topojson-client` are already in node_modules
// (transitively via @malloydata/render's `us-atlas` dep) but ship
// without bundled @types in this monorepo. We use the wide-typed
// surface here rather than pulling in the official @types packages
// because the preview only touches two functions from each module.

declare module "d3-geo" {
   export interface GeoProjection {
      fitSize(size: [number, number], object: unknown): GeoProjection;
   }
   export interface GeoPath {
      (feature: unknown): string | null;
   }
   export function geoAlbersUsa(): GeoProjection;
   export function geoPath(projection: GeoProjection): GeoPath;
}

declare module "topojson-client" {
   export function feature(topology: unknown, object: unknown): unknown;
}

declare module "us-atlas/states-10m.json" {
   const value: unknown;
   export default value;
}
