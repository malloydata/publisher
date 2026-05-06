# Malloy Publisher Python SDK

Auto-generated, fully typed Python client for the Malloy Publisher REST API.

---

## Installation

```bash
pip install malloy-publisher-sdk  # once published on PyPI
```

For local development against the monorepo:

```bash
cd packages/python-client
uv venv .venv        # create virtual env (requires `uv`)
source .venv/bin/activate
uv pip install -e ".[test]"  # editable install with test extras
```

---

## Quick-start

```python
from malloy_publisher_sdk import Client, errors
from malloy_publisher_sdk.api.environments import list_environments

client = Client(base_url="http://localhost:4000/api/v0")

try:
    environments = list_environments.sync(client=client)
    for env in environments:
        print(env.name, env.resource)
except errors.ApiError as exc:
    print(exc.status_code, exc.body)
```

### Async usage

```python
import asyncio
from malloy_publisher_sdk import Client
from malloy_publisher_sdk.api.environments import list_environments

async def main():
    async with Client(base_url="http://localhost:4000/api/v0") as client:
        environments = await list_environments.asyncio(client=client)
        print([e.name for e in environments])

asyncio.run(main())
```

---

## Development

### Regenerating the SDK

When the OpenAPI spec (`api-doc.yaml`) changes, run the helper script and commit the diff:

```bash
cd packages/python-client
scripts/build-python-sdk.sh
```

The script performs:
1. Spec validation
2. Code generation via **openapi-python-client**
3. Version synchronization
4. Formatting, linting, type-checking
5. Test execution

### Building the Package

To build the Python package for distribution:

```bash
# Build only (for testing)
./scripts/build-package.sh

# Or build as part of SDK generation
BUILD_PACKAGE=true ./scripts/build-python-sdk.sh
```

This creates wheel and source distributions in the `dist/` directory.


#### Version Management

The package version is managed in `pyproject.toml`. Update the version there and run the build script to synchronize it across all files.

---

## Contributing

Pull requests welcome! Make sure `./scripts/build-python-sdk.sh` passes before opening a PR.

For packaging changes, also verify:
```bash
./scripts/build-package.sh  # Test package building
pip install dist/*.whl      # Test local installation
```

---

## License

MIT © Malloy Data
