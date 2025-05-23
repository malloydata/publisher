# Publisher: The Malloy Semantic Model Server

[![build](https://github.com/malloydata/publisher/actions/workflows/build.yml/badge.svg)](https://github.com/malloydata/publisher/actions/workflows/build.yml)

Welcome to Publisher, the open-source semantic model server for the [Malloy](https://github.com/malloydata/malloy) data language.

**What is Malloy?**

[Malloy](https://github.com/malloydata/malloy) is a open-source language for modeling data. Malloy allows you to build rich **semantic data models** – defining the _meaning_, _relationships_, and _context_ behind your datas.

Malloy provides a robust framework to encode the business context of your data and running queries against your databases. The accompanying [VS Code extension](https://github.com/malloydata/malloy-vscode-extension) provides an environment for developing Malloy models, exploring data, and building simple dashboards.

**What is Publisher?**

Publisher takes the semantic models defined in Malloy – models rich with business context and meaning – and exposes them through a server interface. This allows applications, **AI agents**, tools, and users to query your data consistently and reliably, leveraging the shared, unambiguous understanding defined in the Malloy model.

**The Goal:**

Publisher solves a growing problem in modern data and AI systems: You can't trust answers if the meaning behind the data isn't clear.

Whether it's a dashboard, a data app, or an AI-agent, every application that interacts with your data needs to understand what terms like "revenue", "active user", or "churn" actually mean in your business. Without that shared understanding, answers are inconsistent at best — and dangerously wrong at worst.

Publisher serves semantic models — defined in Malloy — via APIs. This means:

- You query data in terms of your business, not raw SQL.
- Tools and agents make API calls using Malloy queries, and Publisher compiles them to SQL behind the scenes.
- You get trusted answers from your agent, dashboard, or embedded app — without redefining you data's meaning in every tool.

We believe the semantic layer is the missing keystone of the modern data stack. Publisher and Malloy provide an open-source, developer-first way to define, manage, and serve that layer — enabling consistent, interpretable, and AI-ready data access across all your tools.

Instead of locking your definitions into proprietary BI platforms, you can now ship them like code: versioned, tested, and served through clean interfaces.

## MCP Prompt Capability

Publisher's MCP interface exposes **LLM-ready prompts** for explaining, generating, translating and summarising Malloy code.

| Prompt ID | Purpose |
|-----------|---------|
| `explain-malloy-query@1.0.0` | Explain a Malloy query (sources, transformations, output). |
| `generate-malloy-query-from-description@1.0.0` | Create Malloy based on a natural-language goal and model context. |
| `translate-sql-to-malloy@1.0.0` | Convert SQL into Malloy using a model for schema reference. |
| `summarize-malloy-model@1.0.0` | Summarise a Malloy model (purpose, entities, joins). |

List prompts:
```bash
mcp-client prompts/list
```
Get a prompt:
```bash
mcp-client prompts/get \
  --name explain-malloy-query@1.0.0 \
  --arguments '{"query_code":"from flights"}'
```
These calls return `messages` ready for your LLM chat completion.

## Architecture Overview

Publisher consists of three main components: the Publisher Server (APIs & backend, now including MCP support), the Publisher SDK (UI components), and the Publisher App (a reference data app implementation).

The image below illustrates the composition of the Publisher's components and the tools & applications it can support.

<center>
<img src="publisher.png" width=400>
</center>
<br>

**1. Publisher Server (`packages/server/`)**

- **Core Backend:** This is the heart of Publisher. It's a server application responsible for loading and managing Malloy Packages, which encapsulate your semantic models.
- **Malloy Integration:** It utilizes the Malloy runtime to parse `.malloy` files, understand the rich semantic models defined within them (including relationships, calculations, and business context), and compile Malloy queries into SQL for execution against target databases (BigQuery, Snowflake, Trino, DuckDB, Postgres, MySQL).
- **API Layers:** The Publisher server exposes two primary API interfaces:
  - **REST API:**
    - **Endpoint:** `/api/v0` (running on port defined by `PUBLISHER_PORT`, default `4000`)
    - **Host:** Defined by `PUBLISHER_HOST` (default `localhost`)
    - **Purpose:** Used by the web frontend (Publisher App/SDK) for browsing packages, models, and executing queries.
    - **Specification:** Defined in [`api-doc.yaml`](api-doc.yaml).
    - **Authentication:** None.
  - **Model Context Protocol (MCP) API:**
    - **Endpoint:** `/mcp` (running on port defined by `MCP_PORT`, default `4040`)
    - **Host:** Defined by `PUBLISHER_HOST` (default `localhost`)
    - **Purpose:** Allows AI agents and other MCP clients (like the [MCP Inspector](https://github.com/modelcontextprotocol/inspector) or compatible applications) to interact with Malloy resources (projects, packages, models, sources, views, notebooks) and execute queries programmatically.
    - **Specification:** Adheres to the [MCP `2025-03-26` specification revision](https://modelcontextprotocol.io/specification/2025-03-26/). This includes providing resource metadata and detailed error messages with suggestions.
    - **Transport:** Uses the `StreamableHttpServerTransport` defined in the specification, requiring compatible MCP clients.
    - **Authentication:** None.
    - **Compatibility:** This implementation uses the modern `StreamableHttpServerTransport` and is **not** backward compatible with older clients expecting the deprecated SSE transport ([Source: MCP SSE Transport Deprecation](https://mcp-framework.com/docs/Transports/sse/)).
    - **Usage:** To connect an MCP client, point it to `http://<PUBLISHER_HOST>:<MCP_PORT>/mcp`. See the [MCP Documentation](https://modelcontextprotocol.io/) for client examples.
  - **SQL API (coming soon):**
    - **Purpose:** Connect to you existing tools.
  - **Malloy Package Format:** The Publisher Server loads semantic models, notebooks, and transformations based on the Malloy Package format. This format is designed to integrate seamlessly with standard developer practices.
  - **Goal: Scalability and Governance through Standard Practices:** Enable engineers to manage, version, test, and distribute their data transformations and semantic models using familiar workflows (local development, CI/CD) and distribution mechanisms (e.g., packages, container images, registries). This aims to scale data development far beyond the limitations of current ad-hoc approaches. Crucially, leveraging these standard software engineering practices provides a natural form of **governance**. When a versioned package is pushed by a trusted source to a central repository or registry, that specific version effectively becomes the blessed or "governed" definition for consumption. This contrasts sharply with the complex, often bespoke processes required by traditional data catalogs or BI tools to achieve similar levels of trust and governance for data assets.
  - **Structure:** A Malloy package is currently defined as a directory containing:
    - One or more `.malloy` files defining data models, queries, and transformations.
    - Optionally, one or more `.malloynb` files (Malloy Notebooks) for ad hoc analysis, exploration, and dashboard-like presentation.
    - A `publisher.json` manifest file.
  - **Manifest (`publisher.json`):** Contains metadata about the package. Currently, it supports `name`, `version`, and `description` fields. This schema will be expanded significantly as Publisher evolves to better support dependency management, versioning, and integration with package/container registries, further strengthening the governance model.

**2. Publisher SDK (`packages/sdk/`)**

- **UI Component Library:** A collection of reusable React components designed for building user interfaces that interact with a Publisher Server's _RESTful API_.
- **Embeddable:** Intended to be imported and used within other React-based data applications, allowing developers to easily add Malloy model browsing and querying capabilities for human users.
- **Server Communication:** Handles fetching data and sending query requests to the Publisher Server's REST APIs.

**3. Publisher App (`packages/app/`)**

- **Reference Implementation:** A standalone web application built using the Publisher SDK.
- **Functionality:** Allows users to connect to a running Publisher Server instance (via the REST API), browse the available Malloy packages and their contents, and generate embeddable code snippets.
- **Purpose:** Serves as a practical example of how to use the SDK and provides a useful tool for local development and exploration by human analysts.

**4. MCP-Powered Applications**

The Publisher Server, with its MCP interface exposing Malloy semantic models, enables a new class of data-driven applications, particularly those leveraging AI. For example:

- **AI Data Analysts:** Autonomous agents that can connect to the MCP server, understand the available business metrics and dimensions defined in Malloy, ask complex analytical questions (e.g., "What were the main drivers of customer churn last quarter by region?"), and generate reports or insights based on the semantically consistent data retrieved.
- **Context-Aware Chatbots:** Customer service or internal support chatbots that can query the semantic layer via MCP to answer specific data-related questions accurately (e.g., "What's the current inventory level for product SKU 12345?" or "What is the ARR for customer X?").
- **Automated Reporting & Alerting:** Systems that monitor key metrics defined in the Malloy models via MCP and automatically generate reports or trigger alerts when certain thresholds or anomalies are detected, with full confidence in the definition of the metrics being monitored.
- **Data Quality Validation:** Tools that use the semantic model definitions accessed via MCP to automatically validate data in the underlying warehouse against the expected business rules and definitions.
- **Enhanced BI Tools:** Future BI tools could potentially use MCP as a standard way to connect to semantic layers like Publisher, offering users a more reliable and consistent view of data across different platforms.

## Publisher App Demo

<center>
    <a href="https://www.loom.com/embed/4c1b3313f3624b41be9e16044bebfc38?sid=6dd7879e-19e5-4ee5-8bf1-960cbc46b5eb">
        <img src="malloy-publisher-demo.png" width=600>
    </a>
</center>

## Build and Run Instructions

Follow these steps to build the Publisher components and run the server locally. This project uses [`bun`](https://bun.sh/) as the JavaScript runtime and package manager.

**1. Initialize and Update Git Submodules:**

The Publisher repository uses Git submodules to include sample Malloy models (currently a fork of `malloy-samples`). These samples are used for testing and demonstrating Publisher's capabilities.

First, initialize the registered submodules:

```bash
git submodule init
```

Then, update the submodules to fetch their content:

```bash
git submodule update
```

**2. Install Dependencies:**

Install all necessary project dependencies (including those for the server, SDK, and app) using bun:

```bash
bun install
```

**3. Build the Project:**

Compile the TypeScript code for all packages (server, SDK, app) into JavaScript:

```bash
bun run build
```

**4. Start the Publisher Server:**

Run the compiled server code. By default, this will start the REST API server on port 4000 and the MCP server on port 4040. The server will load the Malloy packages found in the submodules.

```bash
bun run start
```

Once started, you can typically access the Publisher App (if running) at http://localhost:4000 and the MCP endpoint at http://localhost:4040/mcp.

See [packages/app/README.md](packages/app/README.md) for information on how to do development on the server.

**5. (Optional) Configure GCP Credentials for BigQuery Samples:**

Some of the included malloy-samples run queries against Google BigQuery public datasets. To run these specific samples, you need to authenticate with Google Cloud:

Update your Application Default Credentials (ADC) by logging in with gcloud:

```bash
gcloud auth login --update-adc
```

Set your default GCP project (replace {my_project_id} with your actual project ID, though for public datasets, any valid project should generally work):

```bash
gcloud config set project {my_project_id} --installation
```

The Publisher server (specifically the Malloy runtime) will automatically use these credentials when connecting to BigQuery.

## Server Configuration

Publisher uses configuration files on the local filesystem to manage server settings and project-specific details like database connections.

- **Server Configuration (`publisher.config.json`):**

  - **Location:** Stored at the `SERVER_ROOT` directory (the directory from which the `publisher-server` command is run or where the server package is located).
  - **Purpose:** Defines the overall server environment, primarily by listing the available "projects" and their relative paths. A project represents a distinct environment or collection of packages.
  - **Example:** See [`packages/server/publisher.config.json`](packages/server/publisher.config.json) for the basic structure.

- **Project Configuration (`publisher.connections.json`):**

  - **Location:** Stored at the root of each individual project directory defined in the server configuration.
  - **Purpose:** Contains project-specific settings, most importantly the database connection configurations (credentials, database names, types like BigQuery/Postgres/DuckDB, etc.) required by the Malloy models within that project's packages.
  - **Example:** See [`malloy-samples/publisher.connections.json`](packages/server/malloy-samples/publisher.connections.json) for an example.

- **Environment Management:**

  - This two-tiered configuration structure (server-level listing projects, project-level defining connections) allows for standard environment separation (e.g., `dev`, `staging`, `prod`), a common practice in cloud development.
  - You can create separate project directories for each environment. Each project directory would contain its own `publisher.connections.json` with the appropriate credentials for that environment.
  - Crucially, these environment-specific project directories can reference the _same_ underlying Malloy packages (containing the models and notebooks) using symbolic links.

  - **Example File Structure:**
    ```
    SERVER_ROOT/
    ├── publisher.config.json       # Lists 'staging' and 'prod' projects
    │
    ├── packages/                   # Contains the actual Malloy packages
    │   ├── package1/
    │   │   └── model.malloy
    │   ├── package2/
    │   └── ...
    │
    ├── staging/                    # Staging environment project
    │   ├── publisher.connections.json # Staging DB credentials
    │   ├── package1 -> ../packages/package1  # Symbolic link
    │   └── package2 -> ../packages/package2  # Symbolic link
    │
    └── prod/                       # Production environment project
        ├── publisher.connections.json  # Production DB credentials
        ├── package1 -> ../packages/package1   # Symbolic link
        └── package2 -> ../packages/package2   # Symbolic link
    ```
  - **Benefit:** This allows you to build a single Docker image containing the Publisher server and all Malloy packages. You can then deploy this _same image_ to different environments (staging, production). By configuring your staging and productio jobs to point to the appropriate project (`staging` or `prod`), you ensure the correct connection credentials are used for each environment without rebuilding the image or modifying the core package code.

### Upgrading Malloy dependencies

To update to a new NPM release of `@malloydata/*`:

```
bun run upgrade-malloy 0.0.XXX #XXX is the new version number
bun install # This updates node_modules
```

> **_NOTE:_** Note that the Publisher repository currently points to a [fork](https://github.com/pathwaysdata/malloy-samples) of the [malloy-samples](https://github.com/malloydata/malloy-samples) repo. The fork contains minor changes to turn each Malloy sample directory into a package. Once the package format solidifies, we intend to merge the changes into the main malloy-samples repo.

## Coming Soon

We are actively developing Publisher and plan to introduce several exciting features:

- **Enhanced Developer Mode:** A streamlined local development experience where changes to your `.malloy` or `.malloynb` files automatically trigger recompilation of models and hot-reloading of the Publisher App/SDK, enabling faster iteration and testing.
- **Integrated Ad Hoc Analysis UI:** Embed the powerful [Explore UI from Malloy Composer](https://github.com/malloydata/malloy-composer) directly within the Publisher App. This will provide a rich, graphical interface for interactively querying and visualizing data from published Malloy models without needing to write code.
- **Scheduled Transform Pipelines:** Extend Publisher to orchestrate the execution of Malloy transformations on a schedule. Define pipelines within your Malloy packages to update materialized views, create summary tables, or perform other routine data preparation tasks directly managed by Publisher.
- **SQL API:** Connect your existing BI & data tools to Malloy semantic models via a Postgres SQL API.
- **Containerization Support (Dockerfile & Images):** Provide official Dockerfiles and pre-built container images to easily package the Publisher server along with specific Malloy packages. This simplifies deployment, promotes consistency across environments, and aligns with standard DevOps practices.
- **DBT Integration:** Bridge the gap with the popular dbt ecosystem. Potential integration points include referencing Malloy models within dbt and triggering Malloy transformations as part of dbt workflows.
- **Airflow Integration:** Enable seamless integration with Apache Airflow. This could involve custom Airflow operators to trigger Publisher actions like model refreshes or scheduled pipeline runs, allowing Malloy/Publisher tasks to be incorporated into larger, complex data orchestration DAGs.

## Join the Malloy Community

- Join our [**Malloy Slack Community!**](https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw) Use this community to ask questions, meet other Malloy users, and share ideas with one another.
- Use [**GitHub issues**](https://github.com/malloydata/publisher/issues) in this Repo to provide feedback, suggest improvements, report bugs, and start new discussions.

## Resources

Documentation:

- [Malloy Language](https://malloydata.github.io/malloy/documentation/language/basic.html) - A quick introduction to the language
- [eCommerce Example Analysis](https://malloydata.github.io/malloy/documentation/examples/ecommerce.html) - a walkthrough of the basics on an ecommerce dataset (BigQuery public dataset)
- [Modeling Walkthrough](https://malloydata.github.io/malloy/documentation/examples/iowa/iowa.html) - introduction to modeling via the Iowa liquor sales public data set (BigQuery public dataset)
- [YouTube](https://www.youtube.com/channel/UCfN2td1dzf-fKmVtaDjacsg) - Watch demos / walkthroughs of Malloy
