// Central list of the Malloy documentation URLs the Publisher UI links to.
// Keeping them in one place means a docs move only needs updating here, rather
// than hunting down hardcoded links across the app.

const DOCS_BASE = "https://docs.malloydata.dev/documentation";
const PUBLISHING_BASE = `${DOCS_BASE}/user_guides/publishing`;
// Publisher's own reference docs, which live in the repo rather than on the
// docs site. Newer features are documented here first, and for some of them
// this is the only write-up there is.
//
// Pinned to `main`, so a link is live the moment its doc merges and dead until
// then. `docLinks.spec.ts` checks each target exists in this repo, which catches
// a rename or a deletion but cannot catch a doc that is here and not yet on
// `main`; adding a key for a doc still on a branch is a sequencing decision, not
// something the build can see.
//
// Two of these are in that state right now and both resolve on the way in:
// `dashboards.md` arrives with the dashboards slice this branch is stacked on,
// and `choosing-a-surface.md` ships in this change. Merging ahead of that slice
// would put a 404 behind the Dashboards card.
const REPO_DOCS = "https://github.com/malloydata/publisher/blob/main/docs";

export const DOC_LINKS = {
   // Documentation landing page.
   docsHome: `${DOCS_BASE}/`,
   // Setup, deployment, configuration, and the publisher.json package format.
   publishing: `${PUBLISHING_BASE}/publishing`,
   // No-code visual query builder.
   explorer: `${PUBLISHING_BASE}/explorer`,
   // Connect Claude and other AI assistants over MCP.
   mcpAgents: `${PUBLISHING_BASE}/mcp_agents`,
   // Notebooks, dashboards, and HTML data apps, and how to pick between them.
   surfaces: `${REPO_DOCS}/choosing-a-surface.md`,
   // Dashboards declared as tags on a Malloy query.
   dashboards: `${REPO_DOCS}/dashboards.md`,
   // No-build HTML pages served from a package's public/ directory.
   dataApps: `${REPO_DOCS}/html-data-apps.md`,
   // Runtime parameters: filter controls, row-level access, source gates.
   givens: `${REPO_DOCS}/givens.md`,
   // Persisting an expensive source into a table, on demand or on a cron.
   materialization: `${REPO_DOCS}/materialization.md`,
   // Database and query-engine connections.
   connections: `${REPO_DOCS}/connections.md`,
} as const;
