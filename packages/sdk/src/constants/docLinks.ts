// Central list of the Malloy documentation URLs the Publisher UI links to.
// Keeping them in one place means a docs move only needs updating here, rather
// than hunting down hardcoded links across the app.

const DOCS_BASE = "https://docs.malloydata.dev/documentation";
const PUBLISHING_BASE = `${DOCS_BASE}/user_guides/publishing`;

export const DOC_LINKS = {
   // Documentation landing page.
   docsHome: `${DOCS_BASE}/`,
   // Setup, deployment, configuration, and the publisher.json package format.
   publishing: `${PUBLISHING_BASE}/publishing`,
   // No-code visual query builder.
   explorer: `${PUBLISHING_BASE}/explorer`,
   // Connect Claude and other AI assistants over MCP.
   mcpAgents: `${PUBLISHING_BASE}/mcp_agents`,
} as const;
