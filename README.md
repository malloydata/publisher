# Malloy Publisher: A Semantic Model Serving and Development Platform

[![build](https://github.com/malloydata/publisher/actions/workflows/build.yml/badge.svg)](https://github.com/malloydata/publisher/actions/workflows/build.yml)

Malloy Publisher is an open-source project designed to simplify the development and deployment of data applications built on semantic models programmed in [**Malloy**](https://github.com/malloydata/malloy). We aim for it to provide a comprehensive ecosystem for exploring, managing, and serving Malloy packages, empowering developers to build rich, data-driven experiences with ease.

## The Power of Semantic Modeling

In today's data-rich environment, raw data alone is often insufficient. To unlock its true potential, data needs to be refined, organized, and understood in a meaningful context. This is where semantic modeling comes in.
Semantic models act as a crucial intermediary layer between raw data and data consumers (applications, analysts, etc.). They provide:

* Abstraction: Hiding the complexities of underlying data storage and query languages, allowing users to focus on business logic and data relationships.
* Governance: Enforcing consistent definitions, business rules, and access controls across your data landscape.
* Reusability: Defining data entities and relationships that can be reused across multiple applications and analyses, reducing redundancy and promoting consistency.
* Clarity: Providing a clear, human-understandable representation of your data, making it easier for both technical and non-technical users to understand and work with.

Malloy, an open-source semantic modeling language, provides a powerful foundation for building these models. Malloy Publisher builds upon this foundation to offer a semantic layer for leveraging semantic models in real-world applications.

## Introducing Malloy Publisher

Malloy Publisher is comprised of three core components:

* [Publisher Server](packages/sdk): A robust, lightweight server that hosts and serves Malloy packages via a well-defined [APIs](api-doc.yaml). It acts as a semantic layer, providing access to model definitions, package metadata, and query execution capabilities.
* [Publisher SDK](packages/sdk): A React component library (@malloy-publisher/sdk) that simplifies embedding Malloy models and query results into data applications. It provides pre-built UI components for exploring and visualizing Malloy data.
* [Publisher App](packages/app/): A user-friendly web application built with the SDK, providing a visual interface for browsing packages, exploring models and notebooks, and generating embeddable code snippets for the SDK.

<img src="publisher.png" width=400>

Currently, a Malloy package consists of a set of files in a directory with a publisher.json file.  The publisher.json only supports two fields at the moment (name & description).  We intend to add more fields as we build out Publisher functionality.

## Key Features and Benefits

* **Package Exploration:** Browse and explore loaded Malloy packages, their models, notebooks, and embedded databases through the intuitive Publisher App

<center>
    <figcaption>Browse loaded packages</figcaption>
    <img src="project-screenshot.png" width=800>
</center>
<br>

* **Model and Notebook Visualization:** Render Malloy models and notebooks directly in the browser, gaining insights into their structure and queries.

<center>
    <figcaption>Explore a package's contents</figcaption>
    <img src="package-screenshot.png" width=800>
</center>
<br>
<center>
    <figcaption>Explore Malloy models and notebooks</figcaption>
    <img src="notebook-screenshot.png" width=800>
</center>

* **Embeddable SDK Components:** Utilize the @malloy-publisher/sdk React library to seamlessly embed interactive Malloy components into your own data applications. Easily display query results, models, and notebooks with minimal code.
* **API-Driven Access:** Interact with the Publisher Server programmatically via its comprehensive REST API. This allows you to build custom integrations and data applications that leverage the power of Malloy semantic models.

## Getting Started

To build and run the Malloy Publisher, follow these steps:

**Prerequisites**

* Install Node.js and npm: Ensure you have Node.js (version >=20) and npm (version >=10) installed on your system.

* Clone the Repository:
```
git clone https://github.com/malloydata/publisher.git
cd publisher
```

* Initialize Submodules: Load the malloy-samples submodule:

```
git submodule init
git submodule update
```

**Building and Running the Publisher**

* Install Dependencies:

```
npm install
```

* Build the Codebase:

```
npm run build
```

* Start the Publisher Server:

```
cd packages/server
npm run start
```

The server will start and be accessible at http://localhost:4000. The Publisher App will be served at the same address.

## Using the SDK in Your Application

* Install the SDK Package:

```
npm install @malloy-publisher/sdk
```

* Import Components: Import the desired components from @malloy-publisher/sdk in your React application:

```
import { QueryResult } from '@malloy-publisher/sdk';
```

* Embed Components: The publisher UI generates embeddable links. Use these components in your JSX to display Malloy content. For example, to embed a QueryResult:

```
<QueryResult
           server="http://localhost:4000/api/v0" // Replace with your Publisher Server URL if different
           projectName="your-project-name"
           packageName="your-package-name"
           modelPath="path/to/your/model.malloy"
           queryName="your_query_name"
           />
```

Refer to the SDK documentation for more detailed usage instructions and component options.


> **_NOTE:_**  Note that the Publisher repository currently points to a [fork](https://github.com/pathwaysdata/malloy-samples) of the [malloy-samples](https://github.com/malloydata/malloy-samples) repo.  The fork contains minor changes to turn each Malloy sample directory into a package.  Once the package format solidifies, we intend to merge the changes into the main malloy-samples repo.

## Coming Soon

* Developer mode that automatically recompiles models and refreshes the publisher app as you make changes
* Embed Composer's [Explore UI](https://github.com/malloydata/malloy-composer) to enable ad hoc anslysis of packages via a UI
* Scheduled transform pipelines
* Scheduled report generation
* Dockerfile
* In-browser
* DBT integration
* Ariflow integration

---

## Join the Malloy Community

- Join our [**Malloy Slack Community!**](https://join.slack.com/t/malloy-community/shared_invite/zt-1kgfwgi5g-CrsdaRqs81QY67QW0~t_uw) Use this community to ask questions, meet other Malloy users, and share ideas with one another.
- Use [**GitHub issues**](https://github.com/malloydata/publisher/issues) in this Repo to provide feedback, suggest improvements, report bugs, and start new discussions.

## Resources

Documentation:

- [Malloy Language](https://malloydata.github.io/malloy/documentation/language/basic.html) - A quick introduction to the language
- [eCommerce Example Analysis](https://malloydata.github.io/malloy/documentation/examples/ecommerce.html) - a walkthrough of the basics on an ecommerce dataset (BigQuery public dataset)
- [Modeling Walkthrough](https://malloydata.github.io/malloy/documentation/examples/iowa/iowa.html) - introduction to modeling via the Iowa liquor sales public data set (BigQuery public dataset)
- [YouTube](https://www.youtube.com/channel/UCfN2td1dzf-fKmVtaDjacsg) - Watch demos / walkthroughs of Malloy
