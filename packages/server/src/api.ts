/**
 * This file was auto-generated by openapi-typescript.
 * Do not make direct changes to the file.
 */


export interface paths {
  "/about": {
    /** Returns metadata about the publisher service. */
    get: operations["about"];
  };
  "/packages": {
    /** Returns a list of the Packages hosted on this server. */
    get: operations["list-packages"];
  };
  "/packages/{name}": {
    /** Returns the package metadata. */
    get: operations["get-package"];
  };
  "/packages/{name}/models": {
    /** Returns a list of relative paths to the models in the package. */
    get: operations["list-models"];
  };
  "/packages/{name}/models/{path}": {
    /** Returns a Malloy model. */
    get: operations["get-model"];
  };
  "/packages/{name}/queryResults/{path}": {
    /** Returns a query and its results. */
    get: operations["execute-query"];
  };
  "/packages/{name}/databases": {
    /** Returns a list of relative paths to the databases embedded in the package. */
    get: operations["list-databases"];
  };
}

export type webhooks = Record<string, never>;

export interface components {
  schemas: {
    About: {
      /** @description Readme markdown. */
      readme?: string;
    };
    Package: {
      /** @description Package name. */
      name?: string;
      /** @description Package description. */
      description?: string;
    };
    /** @description Malloy model def and result data.  Malloy model def and result data is Malloy version depdendent. */
    Model: {
      /** @description Model's package Name */
      packageName?: string;
      /** @description Model's relative path in its package directory. */
      path?: string;
      /**
       * @description Type of malloy model file -- source file or notebook file.
       * @enum {string}
       */
      type?: "source" | "notebook";
    };
    /** @description Malloy model def and result data.  Malloy model def and result data is Malloy version depdendent. */
    CompiledModel: {
      /** @description Model's package Name */
      packageName?: string;
      /** @description Model's relative path in its package directory. */
      path?: string;
      /**
       * @description Type of malloy model file -- source file or notebook file.
       * @enum {string}
       */
      type?: "source" | "notebook";
      /** @description Version of the Malloy compiler that generated the model def and results fields. */
      malloyVersion?: string;
      /** @description Data style for rendering query results. */
      dataStyles?: string;
      /** @description Malloy model def. */
      modelDef?: string;
      /** @description Array of model sources. */
      sources?: components["schemas"]["Source"][];
      queries?: components["schemas"]["Query"][];
      /** @description Array of notebook cells. */
      notebookCells?: components["schemas"]["NotebookCell"][];
    };
    /** @description Model source. */
    Source: {
      /** @description Source's name. */
      name?: string;
      /** @description Description of the source. */
      description?: string;
      /** @description List of views in the source.\ */
      views?: components["schemas"]["View"][];
    };
    /** @description Named model view. */
    View: {
      /** @description View's name. */
      name?: string;
      /** @description Description of the view. */
      description?: string;
      /** @description View's code. */
      view?: string;
    };
    /** @description Named model query. */
    Query: {
      /** @description Query's name. */
      name?: string;
      /** @description Description of the query. */
      description?: string;
      /** @description Query's code. */
      code?: string;
    };
    /** @description Notebook cell. */
    NotebookCell: {
      /**
       * @description Type of notebook cell.
       * @enum {string}
       */
      type?: "markdown" | "code";
      /** @description Text contents of the notebook cell. */
      text?: string;
      /** @description Name of query, if this is a named query.  Otherwise, empty. */
      queryName?: string;
      /** @description Malloy query results. Populated only if a code cell. */
      queryResult?: string;
    };
    /** @description A Malloy query's results, its model def, and its data styles. */
    QueryResult: {
      /** @description Data style for rendering query results. */
      dataStyles?: string;
      /** @description Malloy model def. */
      modelDef?: string;
      /** @description Malloy query results. Populated only if a code cell. */
      queryResult?: string;
    };
    /** @description An in-memory DuckDB database embedded in the package. */
    Database: {
      /** @description Database's relative path in its package directory. */
      path?: string;
      /** @description Size of the embedded database in bytes. */
      size?: number;
      /**
       * @description Type of database -- i.e., embedded in the package or materialized afte the package was created.
       * @enum {string}
       */
      type?: "embedded" | "materialized";
    };
    Connection: {
      name?: string;
      /** @enum {string} */
      type?: "postgres" | "bigquery" | "snowflake";
      postgresConnection?: components["schemas"]["PostgresConnection"];
    };
    PostgresConnection: {
      host?: string;
      port?: number;
      databaseName?: string;
      userName?: string;
      password?: string;
      url?: string;
    };
    Error: {
      code?: string;
      message?: string;
    };
  };
  responses: {
    /** @description The server encountered an internal error */
    InternalServerError: {
      content: {
        "application/json": components["schemas"]["Error"];
      };
    };
    /** @description The specified resource was not found */
    NotFoundError: {
      content: {
        "application/json": components["schemas"]["Error"];
      };
    };
    /** @description Not implemented */
    NotImplementedError: {
      content: {
        "application/json": components["schemas"]["Error"];
      };
    };
    /** @description Unauthorized */
    UnauthorizedError: {
      content: {
        "application/json": components["schemas"]["Error"];
      };
    };
    /** @description Bad request */
    BadRequestError: {
      content: {
        "application/json": components["schemas"]["Error"];
      };
    };
  };
  parameters: never;
  requestBodies: never;
  headers: never;
  pathItems: never;
}

export type $defs = Record<string, never>;

export type external = Record<string, never>;

export interface operations {

  /** Returns metadata about the publisher service. */
  about: {
    responses: {
      /** @description Metadata about the publisher service. */
      200: {
        content: {
          "application/json": components["schemas"]["About"];
        };
      };
      401: components["responses"]["UnauthorizedError"];
      500: components["responses"]["InternalServerError"];
    };
  };
  /** Returns a list of the Packages hosted on this server. */
  "list-packages": {
    responses: {
      /** @description A list of the Packages names. */
      200: {
        content: {
          "application/json": components["schemas"]["Package"][];
        };
      };
      401: components["responses"]["UnauthorizedError"];
      500: components["responses"]["InternalServerError"];
      501: components["responses"]["NotImplementedError"];
    };
  };
  /** Returns the package metadata. */
  "get-package": {
    parameters: {
      header?: {
        "X-Version-Id"?: string;
      };
      path: {
        /** @description Package name */
        name: string;
      };
    };
    responses: {
      /** @description Package metadata. */
      200: {
        content: {
          "application/json": components["schemas"]["Package"];
        };
      };
      401: components["responses"]["UnauthorizedError"];
      404: components["responses"]["NotFoundError"];
      500: components["responses"]["InternalServerError"];
      501: components["responses"]["NotImplementedError"];
    };
  };
  /** Returns a list of relative paths to the models in the package. */
  "list-models": {
    parameters: {
      header?: {
        "X-Version-Id"?: string;
      };
      path: {
        /** @description Name of package */
        name: string;
      };
    };
    responses: {
      /** @description A list of relative paths to the models in the package. */
      200: {
        content: {
          "application/json": components["schemas"]["Model"][];
        };
      };
      401: components["responses"]["UnauthorizedError"];
      404: components["responses"]["NotFoundError"];
      500: components["responses"]["InternalServerError"];
      501: components["responses"]["NotImplementedError"];
    };
  };
  /** Returns a Malloy model. */
  "get-model": {
    parameters: {
      header?: {
        "X-Version-Id"?: string;
      };
      path: {
        /** @description Name of package. */
        name: string;
        /** @description Path to model wihin the package. */
        path: string;
      };
    };
    responses: {
      /** @description A Malloy model. */
      200: {
        content: {
          "application/json": components["schemas"]["CompiledModel"];
        };
      };
      401: components["responses"]["UnauthorizedError"];
      404: components["responses"]["NotFoundError"];
      500: components["responses"]["InternalServerError"];
      501: components["responses"]["NotImplementedError"];
    };
  };
  /** Returns a query and its results. */
  "execute-query": {
    parameters: {
      query?: {
        /** @description Query string to execute on the model.  If the query is paramter is set, the queryName parameter must be empty. */
        query?: string;
        /** @description Name of the source in the model to use for queryName, search, and topValue requests. */
        sourceName?: string;
        /** @description Name of a query to execute on a source in the model.  Requires the sourceName parameter is set.  If the queryName is paramter is set, the query parameter must be empty. */
        queryName?: string;
      };
      header?: {
        "X-Version-Id"?: string;
      };
      path: {
        /** @description Name of package */
        name: string;
        /** @description Path to model within the package. */
        path: string;
      };
    };
    responses: {
      /** @description A query and its results. */
      200: {
        content: {
          "application/json": components["schemas"]["QueryResult"];
        };
      };
      400: components["responses"]["BadRequestError"];
      401: components["responses"]["UnauthorizedError"];
      404: components["responses"]["NotFoundError"];
      500: components["responses"]["InternalServerError"];
      501: components["responses"]["NotImplementedError"];
    };
  };
  /** Returns a list of relative paths to the databases embedded in the package. */
  "list-databases": {
    parameters: {
      header?: {
        "X-Version-Id"?: string;
      };
      path: {
        /** @description Name of package */
        name: string;
      };
    };
    responses: {
      /** @description A list of relative paths to the databases embedded in the package. */
      200: {
        content: {
          "application/json": components["schemas"]["Database"][];
        };
      };
      401: components["responses"]["UnauthorizedError"];
      404: components["responses"]["NotFoundError"];
      500: components["responses"]["InternalServerError"];
      501: components["responses"]["NotImplementedError"];
    };
  };
}