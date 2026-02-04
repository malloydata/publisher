# Malloy Publisher CLI

Command-line interface for managing Malloy Publisher resources.

## Installation
```bash
npm install -g @malloydata/publisher-cli
```

## Usage
```bash
# Set the Publisher server URL (optional, defaults to http://localhost:4000)
export MALLOY_PUBLISHER_URL=http://localhost:4000

# List projects
malloy-pub list project

# Create a project
malloy-pub create project my-project

# Update a project
malloy-pub update project my-project --readme "Updated readme"

# Create connections from file
malloy-pub create connection --project my-project --file connections.json
```

## Development
```bash
# Install dependencies
npm install

# Generate API client from OpenAPI spec
npm run generate:api

# Build
npm run build

# Link for local development
npm link

# Test
malloy-pub list project --url http://localhost:4000
```

## Commands

- `malloy-pub list <resource>` - List resources (project, package, connection)
- `malloy-pub get <resource> <name>` - Get resource details
- `malloy-pub create <resource> <name>` - Create a resource
- `malloy-pub update <resource> <name>` - Update a resource
- `malloy-pub delete <resource> <name>` - Delete a resource