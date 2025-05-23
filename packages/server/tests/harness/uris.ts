export interface FixtureUris {
  project: string;
  package: string;
  flightsModel: string;
  source: string;
  view: string;
  query: string;
  notebook: string;
}

/**
 * Return canonical URIs used across integration tests, parametrised by project
 * and package. Most suites will call this with defaults.
 */
export function malloyUris(
  project = "home",
  pkg = "faa",
): FixtureUris {
  const base = `malloy://project/${project}`;
  const pkgUri = `${base}/package/${pkg}`;
  const model = `${pkgUri}/models/flights.malloy`;
  return {
    project: base,
    package: pkgUri,
    flightsModel: model,
    source: `${model}/sources/flights`,
    view: `${model}/sources/flights/views/flights_by_month`,
    query: `${model}/queries/flights_by_carrier`,
    notebook: `${pkgUri}/notebooks/overview.malloynb`,
  };
} 