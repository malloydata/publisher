export const MCP_ERROR_MESSAGES = {
  // Protocol-level errors (validation)
  MISSING_REQUIRED_PARAMS: "Either 'query' or both 'sourceName' and 'queryName' must be provided",
  MUTUALLY_EXCLUSIVE_PARAMS: "Cannot provide both 'query' and 'queryName'",
  INVALID_ARGUMENTS: (toolName: string, errors: string[]) => 
    `Invalid arguments for tool ${toolName}: ${errors.join(", ")}`,
  
  // Application-level errors (runtime)
  PROJECT_NOT_FOUND: (projectName: string) =>
    `Project '${projectName}' is not available or does not exist.`,
  PACKAGE_NOT_FOUND: (packageName: string) => 
    `Package manifest for ${packageName} does not exist.`,
  MODEL_NOT_FOUND: (packageName: string, modelPath: string) => 
    `Model '${modelPath}' not found in package '${packageName}'`,
  QUERY_NOT_FOUND: (queryName: string) => 
    `'${queryName}' is not defined`,
  COMPILATION_ERROR: (error: string) => 
    `Error(s) compiling model:\n${error}`,
  
  // Error message prefixes
  ERROR_EXECUTING_QUERY: (packagePath: string) => 
    `Error executing query on '${packagePath}':`,
} as const; 