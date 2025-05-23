export interface ErrorDetails {
   message: string;
   suggestions: string[];
}

export interface InvalidParamDetail {
   name: string;
   reason: string;
   value?: unknown;
}

export function getInvalidParamsError(
   params: InvalidParamDetail[],
): ErrorDetails {
   const reasons = params
      .map(
         (p) =>
            `${p.name}: ${p.reason}${p.value !== undefined ? ` (value: ${JSON.stringify(p.value)})` : ""}`,
      )
      .join("; ");
   return {
      message: `Invalid parameter(s) provided. ${reasons}`,
      suggestions: [
         "Check the parameter names and values against the prompt's argument definition.",
         "Ensure all required parameters are provided and have the correct data types.",
         "For URI parameters, ensure they are correctly formatted.",
      ],
   };
}

/**
 * Generates error details for a resource not found scenario.
 * @param resourceUriOrContext The URI that was not found, or a context string (e.g., "Package 'X'", "Model 'Y' in package 'X'").
 * @returns ErrorDetails object with message and suggestions.
 */
export function getNotFoundError(resourceUriOrContext: string): ErrorDetails {
   const baseMessage = `Resource not found: ${resourceUriOrContext}`;
   const suggestions: string[] = [
      `Verify the identifier or URI (${resourceUriOrContext}) is spelled correctly and exists. Check capitalization and path separators.`,
      `If using a URI, ensure it follows the correct format (e.g., malloy://project/...) and includes the right path segments (e.g., /models/, /sources/, /queries/, /views/).`,
   ];

   suggestions.push(
      "Check if the resource exists and is correctly named in your Malloy project structure or the specific model file.",
   );

   return {
      message: baseMessage,
      suggestions: suggestions,
   };
}

/**
 * Generates generic error details for internal server errors.
 * @param operation The operation that failed (e.g., 'GetResource', 'ListResources').
 * @param error Optional: The underlying error object or message.
 * @returns ErrorDetails object.
 */
export function getInternalError(
   operation: string,
   error?: unknown,
): ErrorDetails {
   const baseMessage = `An unexpected internal error occurred during ${operation}.`;
   const errorMessage = error instanceof Error ? error.message : String(error);
   return {
      message: error ? `${baseMessage}: ${errorMessage}` : baseMessage,
      suggestions: [
         "Try the request again later.",
         "If the problem persists, check server logs or contact support.",
      ],
   };
}

/**
 * Generates detailed error information for Malloy compilation or query execution errors.
 * @param operation The operation that failed (e.g., 'GetResource (model)', 'executeQuery').
 * @param modelIdentifier A path or URI identifying the model/notebook involved.
 * @param error The underlying Malloy error object or other error.
 * @returns ErrorDetails object.
 */
export function getMalloyErrorDetails(
   operation: string,
   modelIdentifier: string,
   error: unknown,
): ErrorDetails {
   let baseMessage = `Error during ${operation} for resource '${modelIdentifier}'.`;
   const initialSuggestions: string[] = [
      `Verify the structure and syntax within the Malloy file: ${modelIdentifier}. Check for typos in source, query, or view names (e.g., 'source: my_source is ...', 'query: my_query is ...', 'view: my_view is { ... }'). See Model Structure: https://docs.malloydata.dev/documentation/language/statement`,
      "Ensure all referenced sources, queries, or views are correctly defined within the model or imported files using expected keywords (e.g., 'source:', 'query:', 'view:'). See Sources: https://docs.malloydata.dev/documentation/language/source",
      "Check the database connection configuration used by the model, if applicable.",
      "Consult the Malloy language documentation for syntax rules: https://docs.malloydata.dev/documentation/",
   ];

   let suggestions: string[] = [...initialSuggestions]; // Clone initial suggestions
   let refined = false; // Flag to track if specific suggestions were added

   // Attempt to extract more specific info if it's a MalloyError or similar
   if (error instanceof Error) {
      // Prepend the specific error message
      baseMessage = `Error during ${operation} for resource '${modelIdentifier}': ${error.message}`;

      // --- Add more specific suggestions based on common Malloy error patterns ---
      // Note: Order matters, check for more specific patterns first.

      const viewNotFoundMatch = error.message.match(
         /View '([^']+)' not found in source '([^']+)'/i,
      );
      const sourceNotFoundMatch = error.message.match(
         /Source '([^']+)' not found/i,
      );
      const queryNotFoundMatch = error.message.match(
         /Query '([^']+)' not found/i,
      );
      const referenceErrorMatch = error.message.match(
         /Reference to undefined object(?: '([^']+)')?/i,
      );
      const syntaxErrorMatch = error.message.match(
         /no viable alternative at input/i,
      );
      const connectionErrorMatch = error.message.match(
         /Cannot connect to database/i,
      );
      const fieldNotFoundMatch = error.message.match(
         /Field '([^']+)' not found in (source|query|view) '([^']+)'/i,
      );
      const invalidRequestMatch = error.message.match(
         /Invalid query request\\. Query OR queryName must be defined/i,
      );

      if (viewNotFoundMatch) {
         refined = true;
         const [, viewName, sourceName] = viewNotFoundMatch;
         suggestions.unshift(
            `Suggestion: View '${viewName}' was not found in source '${sourceName}'. Check the view name spelling or try requesting the resource details for the source URI (e.g., 'malloy://.../sources/${sourceName}') to see the list of available views. Views are defined within sources like 'source: ${sourceName} is ... extend { view: ${viewName} is { ... } }'.`,
         );
      } else if (sourceNotFoundMatch) {
         refined = true;
         const [, sourceName] = sourceNotFoundMatch;
         suggestions.unshift(
            `Suggestion: Source '${sourceName}' was not found or could not be accessed. Verify its definition (e.g., \`source: ${sourceName} is table('...')\` or \`duckdb.sql("...")\`) and ensure any associated connections are valid.`,
            `Suggestion: Check the spelling of '${sourceName}'. You can list sources in the model using ListResources('malloy://.../${modelIdentifier}').`,
         );
      } else if (queryNotFoundMatch) {
         refined = true;
         const [, queryName] = queryNotFoundMatch;
         suggestions.unshift(
            `Suggestion: Named query '${queryName}' was not found. Verify its definition (e.g., \`query: ${queryName} is source_name -> { ... }\`) within the model '${modelIdentifier}'.`,
            `Suggestion: Check the spelling of '${queryName}'. Ensure it's a named query (defined with \`query:\`), not a view. You can list named queries using ListResources('malloy://.../${modelIdentifier}').`,
         );
      } else if (fieldNotFoundMatch) {
         refined = true;
         const [, fieldName, fieldContextType, fieldContextName] =
            fieldNotFoundMatch;
         suggestions.unshift(
            `Suggestion: Field '${fieldName}' was not found in ${fieldContextType} '${fieldContextName}'. Check the spelling of the field name within the definition of '${fieldContextName}'.`,
            `Suggestion: Ensure the field is defined directly or inherited correctly in the '${fieldContextName}' ${fieldContextType}. You can inspect the ${fieldContextType} definition using GetResource.`,
         );
      } else if (referenceErrorMatch) {
         refined = true;
         const objectName = referenceErrorMatch[1] || "[unknown object]";
         suggestions.unshift(
            `Suggestion: Check if the referenced object '${objectName}' (source, query, view, field, dimension, measure) is defined correctly and spelled correctly (e.g., 'source: my_source is ...', 'query: my_query is ...'). Ensure it's defined *before* being used in the model file.`,
         );
      } else if (syntaxErrorMatch) {
         refined = true;
         suggestions.unshift(
            "Suggestion: There seems to be a syntax error near the location indicated in the error message. Check for misplaced/missing keywords (like 'is', '->', 'extend', 'where', 'aggregate', 'group_by', 'nest'), commas, brackets '{ }', or parentheses '( )'. See Query Syntax: https://docs.malloydata.dev/documentation/language/query",
         );
      } else if (connectionErrorMatch) {
         refined = true;
         suggestions.unshift(
            "Suggestion: Verify the database connection details (credentials, host, database name, connection name in Malloy) associated with this model.",
         );
         suggestions.push(
            "Ensure the database server is running and accessible from the publisher server.",
         );
      } else if (invalidRequestMatch) {
         refined = true;
         suggestions.unshift(
            "Suggestion: This error can occur when specifying a view using 'sourceName' and 'queryName'. Try providing the full query string directly in the 'query' parameter instead (e.g., 'query': 'run: your_source_name->your_view_name').",
            "Suggestion: Also verify you are providing either the 'query' parameter OR the correct combination of 'queryName' (and optionally 'sourceName' for views).",
         );
      }

      // If no specific pattern matched, keep the initial suggestions
      if (!refined) {
         suggestions = initialSuggestions;
      }
   } else {
      // Generic message if it's not a standard Error object
      baseMessage += `: ${String(error)}`;
      suggestions = initialSuggestions; // Use initial for non-errors too
   }

   return {
      message: baseMessage,
      suggestions: suggestions,
   };
}
