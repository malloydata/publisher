export const MCP_ERROR_MESSAGES = {
   // Protocol-level errors (validation)
   MISSING_REQUIRED_PARAMS:
      "Either 'query' or both 'sourceName' and 'queryName' must be provided",
   MUTUALLY_EXCLUSIVE_PARAMS: "Cannot provide both 'query' and 'queryName'",
} as const;
