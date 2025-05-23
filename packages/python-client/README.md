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
from malloy_publisher_sdk.api.projects import list_projects

client = Client(base_url="http://localhost:4000/api/v0")

try:
    projects = list_projects.sync(client=client)
    for proj in projects:
        print(proj.name, proj.resource)
except errors.ApiError as exc:
    print(exc.status_code, exc.body)
```

### Async usage

```python
import asyncio
from malloy_publisher_sdk import Client
from malloy_publisher_sdk.api.projects import list_projects

async def main():
    async with Client(base_url="http://localhost:4000/api/v0") as client:
        projects = await list_projects.asyncio(client=client)
        print([p.name for p in projects])

asyncio.run(main())
```

---

## Regenerating the SDK (maintainers)

When the OpenAPI spec (`api-doc.yaml`) changes, run the helper script and commit the diff:

```bash
cd packages/python-client
scripts/build-python-sdk.sh
```

The script performs:
1. Spec validation
2. Code generation via **OpenAPI Generator** (`library=pydantic-v2`)
3. Formatting, linting, type-checking
4. Test execution

---

## Contributing
Pull requests welcome!  Make sure `./scripts/build-python-sdk.sh` passes before opening a PR.

---

## License
MIT © Malloy Data
