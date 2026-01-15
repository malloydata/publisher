import { PublisherClient } from "./publisher.ts";
import { ProjectsClient } from "./projects.ts";
import { ConnectionsClient } from "./connections.ts";
import { PackagesClient } from "./packages.ts";
import { ModelsClient } from "./models.ts";
import { NotebooksClient } from "./notebooks.ts";
import { DatabasesClient } from "./databases.ts";
import { ConnectionsTestClient } from "./connections-test.ts";
import { WatchModeClient } from "./watch-mode.ts";

const baseUrl = "<BASE_URL>";
const publisherClient = new PublisherClient({ baseUrl });
const projectsClient = new ProjectsClient({ baseUrl });
const connectionsClient = new ConnectionsClient({ baseUrl });
const packagesClient = new PackagesClient({ baseUrl });
const modelsClient = new ModelsClient({ baseUrl });
const notebooksClient = new NotebooksClient({ baseUrl });
const databasesClient = new DatabasesClient({ baseUrl });
const connectionsTestClient = new ConnectionsTestClient({ baseUrl });
const watchModeClient = new WatchModeClient({ baseUrl });

export default function () {
   let project,
      projectName,
      connectionName,
      connection,
      updateConnectionBody,
      schemaName,
      tablePath,
      postSqlsourceBody,
      postQuerydataBody,
      postTemporarytableBody,
      _package,
      packageName,
      path,
      queryRequest,
      cellIndex,
      startWatchRequest;

   /**
    * Get server status and health information
    */

   const getStatusResponseData = publisherClient.getStatus();

   /**
    * List all available projects
    */

   const listProjectsResponseData = projectsClient.listProjects();

   /**
    * Create a new project
    */
   project = {
      resource: "repossess",
      name: "sparse",
      readme: "ick",
      location: "dreamily",
      connections: [],
      packages: [],
   };

   const createProjectResponseData = projectsClient.createProject(project);

   /**
    * Get project details and metadata
    */
   projectName = "hydrolyse";

   const getProjectResponseData = projectsClient.getProject(projectName);

   /**
    * Update project configuration
    */
   projectName = "frantically";
   project = {
      resource: "regarding",
      name: "whether",
      readme: "toady",
      location: "for",
      connections: [],
      packages: [],
   };

   const updateProjectResponseData = projectsClient.updateProject(
      projectName,
      project,
   );

   /**
    * Delete a project
    */
   projectName = "woeful";

   const deleteProjectResponseData = projectsClient.deleteProject(projectName);

   /**
    * List project database connections
    */
   projectName = "diligent";

   const listConnectionsResponseData =
      connectionsClient.listConnections(projectName);

   /**
    * Get connection details
    */
   projectName = "hexagon";
   connectionName = "whereas";

   const getConnectionResponseData = connectionsClient.getConnection(
      projectName,
      connectionName,
   );

   /**
    * Create a new database connection
    */
   projectName = "psst";
   connectionName = "dutiful";
   connection = {
      resource: "unimportant",
      name: "experienced",
      type: "postgres",
      attributes: {
         dialectName: "despite",
         isPool: true,
         canPersist: true,
         canStream: true,
      },
      postgresConnection: {
         host: "disclosure",
         port: 3315240349247500,
         databaseName: "that",
         userName: "from",
         password: "ouch",
         connectionString: "because",
      },
      bigqueryConnection: {
         defaultProjectId: "deliberately",
         billingProjectId: "iterate",
         location: "an",
         serviceAccountKeyJson: "hence",
         maximumBytesBilled: "longboat",
         queryTimeoutMilliseconds: "now",
      },
      snowflakeConnection: {
         account: "satirize",
         username: "beyond",
         password: "but",
         privateKey: "mortally",
         privateKeyPass: "obstruct",
         warehouse: "phew",
         database: "upright",
         schema: "successfully",
         role: "produce",
         responseTimeoutMilliseconds: 7145792300714698,
      },
      trinoConnection: {
         server: "since",
         port: 3861065391914698,
         catalog: "rationalize",
         schema: "ah",
         user: "upside-down",
         password: "sniff",
         peakaKey: "but",
      },
      mysqlConnection: {
         host: "circa",
         port: 1436527561701851,
         database: "equate",
         user: "cruelly",
         password: "overcoat",
      },
      duckdbConnection: {
         attachedDatabases: [],
      },
      motherduckConnection: {
         accessToken: "gee",
         database: "seemingly",
      },
   };

   const createConnectionResponseData = connectionsClient.createConnection(
      projectName,
      connectionName,
      connection,
   );

   /**
    * Update an existing database connection
    */
   projectName = "gosh";
   connectionName = "innocently";
   updateConnectionBody = {
      postgresConnection: {
         host: "lean",
         port: 5507211058963486,
         databaseName: "kissingly",
         userName: "hope",
         password: "dapper",
         connectionString: "colorfully",
      },
      mysqlConnection: {
         host: "ha",
         port: 6615868442549182,
         database: "factorise",
         user: "dreary",
         password: "wisely",
      },
      bigqueryConnection: {
         defaultProjectId: "beautifully",
         billingProjectId: "poppy",
         location: "joyously",
         serviceAccountKeyJson: "ack",
         maximumBytesBilled: "fooey",
         queryTimeoutMilliseconds: "consequently",
      },
      snowflakeConnection: {
         account: "swerve",
         username: "lawmaker",
         password: "after",
         privateKey: "gosh",
         privateKeyPass: "up",
         warehouse: "zowie",
         database: "famously",
         schema: "loftily",
         role: "up",
         responseTimeoutMilliseconds: 2679673390803759,
      },
      duckdbConnection: {
         attachedDatabases: [],
      },
      motherduckConnection: {
         accessToken: "boo",
         database: "absent",
      },
      trinoConnection: {
         server: "truthfully",
         port: 8348473141614417,
         catalog: "ha",
         schema: "humble",
         user: "before",
         password: "finally",
         peakaKey: "yowza",
      },
   };

   const updateConnectionResponseData = connectionsClient.updateConnection(
      projectName,
      connectionName,
      updateConnectionBody,
   );

   /**
    * Delete a database connection
    */
   projectName = "authorized";
   connectionName = "swelter";

   const deleteConnectionResponseData = connectionsClient.deleteConnection(
      projectName,
      connectionName,
   );

   /**
    * List database schemas
    */
   projectName = "scarper";
   connectionName = "but";

   const listSchemasResponseData = connectionsClient.listSchemas(
      projectName,
      connectionName,
   );

   /**
    * List tables in database
    */
   projectName = "unlike";
   connectionName = "minty";
   schemaName = "yesterday";

   const listTablesResponseData = connectionsClient.listTables(
      projectName,
      connectionName,
      schemaName,
   );

   /**
    * Get table details from database
    */
   projectName = "whether";
   connectionName = "mouser";
   schemaName = "graffiti";
   tablePath = "indeed";

   const getTableResponseData = connectionsClient.getTable(
      projectName,
      connectionName,
      schemaName,
      tablePath,
   );

   /**
    * Get SQL source (deprecated)
    */
   projectName = "worriedly";
   connectionName = "aw";

   const getSqlsourceResponseData = connectionsClient.getSqlsource(
      projectName,
      connectionName,
   );

   /**
    * Create SQL source from statement
    */
   projectName = "among";
   connectionName = "cleverly";
   postSqlsourceBody = {
      sqlStatement: "aw",
   };

   const postSqlsourceResponseData = connectionsClient.postSqlsource(
      projectName,
      connectionName,
      postSqlsourceBody,
   );

   /**
    * Execute SQL query
    */
   projectName = "similar";
   connectionName = "up";
   postQuerydataBody = {
      sqlStatement: "helplessly",
   };

   const postQuerydataResponseData = connectionsClient.postQuerydata(
      projectName,
      connectionName,
      postQuerydataBody,
   );

   /**
    * Create temporary table
    */
   projectName = "thin";
   connectionName = "fly";
   postTemporarytableBody = {
      sqlStatement: "eek",
   };

   const postTemporarytableResponseData = connectionsClient.postTemporarytable(
      projectName,
      connectionName,
      postTemporarytableBody,
   );

   /**
    * Create temporary table (deprecated)
    */
   projectName = "boo";
   connectionName = "yippee";

   const getTemporarytableResponseData = connectionsClient.getTemporarytable(
      projectName,
      connectionName,
   );

   /**
    * Get table source information
    */
   projectName = "breastplate";
   connectionName = "sharply";

   const getTablesourceResponseData = connectionsClient.getTablesource(
      projectName,
      connectionName,
   );

   /**
    * Execute SQL query (deprecated)
    */
   projectName = "venom";
   connectionName = "whoever";

   const getQuerydataResponseData = connectionsClient.getQuerydata(
      projectName,
      connectionName,
   );

   /**
    * List project packages
    */
   projectName = "below";

   const listPackagesResponseData = packagesClient.listPackages(projectName);

   /**
    * Create a new package
    */
   projectName = "lashes";
   _package = {
      resource: "failing",
      name: "yowza",
      description: "lieu",
      location: "valiantly",
   };

   const createPackageResponseData = packagesClient.createPackage(
      projectName,
      _package,
   );

   /**
    * Get package details and metadata
    */
   projectName = "uncover";
   packageName = "after";

   const getPackageResponseData = packagesClient.getPackage(
      projectName,
      packageName,
   );

   /**
    * Update package configuration
    */
   projectName = "if";
   packageName = "and";
   _package = {
      resource: "hm",
      name: "finally",
      description: "nocturnal",
      location: "yet",
   };

   const updatePackageResponseData = packagesClient.updatePackage(
      projectName,
      packageName,
      _package,
   );

   /**
    * Delete a package
    */
   projectName = "splash";
   packageName = "successfully";

   const deletePackageResponseData = packagesClient.deletePackage(
      projectName,
      packageName,
   );

   /**
    * List package models
    */
   projectName = "forenenst";
   packageName = "ruddy";

   const listModelsResponseData = modelsClient.listModels(
      projectName,
      packageName,
   );

   /**
    * Get compiled Malloy model
    */
   projectName = "grounded";
   packageName = "like";
   path = "supposing";

   const getModelResponseData = modelsClient.getModel(
      projectName,
      packageName,
      path,
   );

   /**
    * Execute Malloy query
    */
   projectName = "huzzah";
   packageName = "kowtow";
   path = "meanwhile";
   queryRequest = {
      query: "coordination",
      sourceName: "smoggy",
      queryName: "phew",
      versionId: "inborn",
   };

   const executeQueryModelResponseData = modelsClient.executeQueryModel(
      projectName,
      packageName,
      path,
      queryRequest,
   );

   /**
    * List package notebooks
    */
   projectName = "phew";
   packageName = "along";

   const listNotebooksResponseData = notebooksClient.listNotebooks(
      projectName,
      packageName,
   );

   /**
    * Get Malloy notebook cells
    */
   projectName = "repentant";
   packageName = "if";
   path = "regarding";

   const getNotebookResponseData = notebooksClient.getNotebook(
      projectName,
      packageName,
      path,
   );

   /**
    * Execute a specific notebook cell
    */
   projectName = "jacket";
   packageName = "oddly";
   path = "modulo";
   cellIndex = "meadow";

   const executeNotebookCellResponseData = notebooksClient.executeNotebookCell(
      projectName,
      packageName,
      path,
      cellIndex,
   );

   /**
    * List embedded databases
    */
   projectName = "emergent";
   packageName = "ugh";

   const listDatabasesResponseData = databasesClient.listDatabases(
      projectName,
      packageName,
   );

   /**
    * Test database connection configuration
    */
   connection = {
      resource: "whether",
      name: "phew",
      type: "postgres",
      attributes: {
         dialectName: "rightfully",
         isPool: false,
         canPersist: true,
         canStream: true,
      },
      postgresConnection: {
         host: "sun",
         port: 2433874587788422,
         databaseName: "hm",
         userName: "as",
         password: "comparison",
         connectionString: "fog",
      },
      bigqueryConnection: {
         defaultProjectId: "shush",
         billingProjectId: "finally",
         location: "junior",
         serviceAccountKeyJson: "and",
         maximumBytesBilled: "unethically",
         queryTimeoutMilliseconds: "understated",
      },
      snowflakeConnection: {
         account: "inspection",
         username: "intent",
         password: "for",
         privateKey: "stained",
         privateKeyPass: "tepid",
         warehouse: "than",
         database: "a",
         schema: "drat",
         role: "instructor",
         responseTimeoutMilliseconds: 6221971454540274,
      },
      trinoConnection: {
         server: "insecure",
         port: 2799995969010001,
         catalog: "what",
         schema: "lovingly",
         user: "cow",
         password: "ironclad",
         peakaKey: "loftily",
      },
      mysqlConnection: {
         host: "spirit",
         port: 7803797711221079,
         database: "an",
         user: "keenly",
         password: "wallop",
      },
      duckdbConnection: {
         attachedDatabases: [],
      },
      motherduckConnection: {
         accessToken: "incidentally",
         database: "past",
      },
   };

   const testConnectionConfigurationResponseData =
      connectionsTestClient.testConnectionConfiguration(connection);

   /**
    * Get watch mode status
    */

   const getWatchStatusResponseData = watchModeClient.getWatchStatus();

   /**
    * Start file watching
    */
   startWatchRequest = {
      projectName: "soggy",
   };

   const startWatchingResponseData =
      watchModeClient.startWatching(startWatchRequest);

   /**
    * Stop file watching
    */

   const stopWatchingResponseData = watchModeClient.stopWatching();
}
