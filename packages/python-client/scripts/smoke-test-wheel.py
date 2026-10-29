# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

"""Drive a running Publisher through the INSTALLED malloy-publisher-sdk.

build.yml's `python_sdk_smoke_test` job runs this on every PR, once per
supported Python, each time with the interpreter of a fresh venv holding only
the built wheel, by path and with `-I`. Run by path, sys.path[0] is this scripts/ directory, and `-I` drops
PYTHONPATH and the user site, so the source tree cannot satisfy an import:
what is exercised is what a user's `pip install` gets.

The unit tests in tests/ mock httpx, so they show the generated client is
self-consistent. This shows the thing they cannot: that api-doc.yaml, the
generator and a real server agree about the wire format, because every answer
below has to parse into the generated models.

Exits 0 when every check passes and 1 on the first that does not, with a
`FAIL:` line saying which.
"""

import argparse
import importlib.metadata
import json
import sys
from pathlib import Path
from typing import Any, NoReturn, cast

import httpx

import malloy_publisher_sdk
from malloy_publisher_sdk import Client
from malloy_publisher_sdk.api.environments import list_environments
from malloy_publisher_sdk.api.models import execute_query_model, get_model, list_models
from malloy_publisher_sdk.api.packages import list_packages
from malloy_publisher_sdk.api.publisher import get_status
from malloy_publisher_sdk.models import (
    CompiledModel,
    Environment,
    Model,
    Package,
    QueryRequest,
    QueryResult,
    ServerStatus,
)
from malloy_publisher_sdk.models.server_status_operational_state import (
    ServerStatusOperationalState,
)
from malloy_publisher_sdk.types import Response, Unset

DIST_NAME = "malloy-publisher-sdk"


def fail(message: str) -> NoReturn:
    print(f"FAIL: {message}")
    sys.exit(1)


def ok(message: str) -> None:
    print(f"ok: {message}")


def parsed(what: str, response: Response[Any], expected: type) -> Any:
    """The parsed body of a 200, or fail naming what came back instead."""
    if response.status_code != 200:
        fail(f"{what}: HTTP {response.status_code}: {response.content[:500]!r}")
    body = response.parsed
    if isinstance(body, list):
        items = cast(list[Any], body)
        stray = [
            type(item).__name__ for item in items if not isinstance(item, expected)
        ]
        if stray:
            fail(f"{what}: expected a list of {expected.__name__}, got {stray}")
    elif not isinstance(body, expected):
        fail(f"{what}: expected {expected.__name__}, got {type(body).__name__}")
    return body


def check_installed() -> None:
    # A fresh venv, so anything outside its prefix means the import resolved
    # somewhere other than the installed wheel.
    module_file = Path(malloy_publisher_sdk.__file__).resolve()
    if not module_file.is_relative_to(Path(sys.prefix).resolve()):
        fail(f"malloy_publisher_sdk imported from {module_file}, outside {sys.prefix}")
    # build-python-sdk.sh stamps __version__ from pyproject.toml with sed; the
    # wheel's metadata comes from the same file through hatchling. A sed that
    # stopped matching leaves the generator's placeholder in __version__.
    # getattr, so a build that dropped the line fails here by name rather
    # than as an AttributeError traceback.
    stamped = getattr(malloy_publisher_sdk, "__version__", None)
    declared = importlib.metadata.version(DIST_NAME)
    if stamped != declared:
        fail(
            f"__version__ is {stamped!r} but the installed {DIST_NAME} is {declared!r}"
        )
    ok(
        f"{DIST_NAME} {declared} imported from the installed wheel on Python {sys.version.split()[0]}"
    )


def expected_layout(config_path: Path) -> dict[str, list[str]]:
    """Environment name -> package names, as the server's config declares them."""
    config = json.loads(config_path.read_text())
    return {
        environment["name"]: sorted(
            package["name"] for package in environment["packages"]
        )
        for environment in config["environments"]
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Drive a running Publisher through the installed malloy-publisher-sdk."
    )
    parser.add_argument(
        "--base-url", required=True, help="e.g. http://localhost:4000/api/v0"
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="the publisher.config.json the server was started with",
    )
    parser.add_argument("--query-environment", required=True)
    parser.add_argument("--query-package", required=True)
    parser.add_argument(
        "--query-model", required=True, help="model path within the package"
    )
    parser.add_argument(
        "--query", required=True, help="a Malloy query that returns at least one row"
    )
    args = parser.parse_args()

    check_installed()

    client = Client(
        base_url=args.base_url,
        # An undeclared status raises rather than parsing to None, so a
        # response the spec does not describe cannot pass as "no body".
        raise_on_unexpected_status=True,
        # Compiling a model and running a query on a cold server can take a
        # while; httpx's default of 5s is a flake, not a check.
        timeout=httpx.Timeout(120.0),
    )

    status = parsed(
        "GET /status", get_status.sync_detailed(client=client), ServerStatus
    )
    if status.operational_state != ServerStatusOperationalState.SERVING:
        fail(
            f"GET /status: operationalState is {status.operational_state!r}, not serving"
        )
    ok("GET /status parsed into ServerStatus, operationalState serving")

    expected = expected_layout(args.config)
    environments = parsed(
        "GET /environments", list_environments.sync_detailed(client=client), Environment
    )
    served = sorted(environment.name for environment in environments)
    if served != sorted(expected):
        fail(
            f"GET /environments: server lists {served}, {args.config} declares {sorted(expected)}"
        )
    ok(f"GET /environments parsed {len(environments)} Environment(s): {served}")

    compiled = 0
    for environment_name, package_names in expected.items():
        packages = parsed(
            f"GET /environments/{environment_name}/packages",
            list_packages.sync_detailed(environment_name, client=client),
            Package,
        )
        listed = sorted(package.name for package in packages)
        if listed != package_names:
            fail(
                f"GET /environments/{environment_name}/packages: server lists {listed}, config declares {package_names}"
            )
        ok(
            f"GET /environments/{environment_name}/packages parsed {len(packages)} Package(s)"
        )

        for package_name in package_names:
            models = parsed(
                f"GET .../packages/{package_name}/models",
                list_models.sync_detailed(
                    environment_name, package_name, client=client
                ),
                Model,
            )
            for model in models:
                # A model the server itself could not compile reports that
                # here; it is the example's problem, not the client's.
                if not isinstance(model.error, Unset) and model.error:
                    print(
                        f"skip: {package_name}/{model.path} does not compile on the server: {model.error[:200]}"
                    )
                    continue
                parsed(
                    f"GET .../packages/{package_name}/models/{model.path}",
                    get_model.sync_detailed(
                        environment_name, package_name, model.path, client=client
                    ),
                    CompiledModel,
                )
                compiled += 1
            ok(
                f"{package_name}: {len(models)} Model(s) listed, each compiled one parsed into CompiledModel"
            )

    if compiled == 0:
        fail("no model compiled anywhere, so CompiledModel was never exercised")

    result = parsed(
        f"POST .../packages/{args.query_package}/models/{args.query_model}/query",
        execute_query_model.sync_detailed(
            args.query_environment,
            args.query_package,
            args.query_model,
            client=client,
            body=QueryRequest(query=args.query),
        ),
        QueryResult,
    )
    if isinstance(result.result, Unset) or not result.result:
        fail("query: QueryResult.result is empty")
    try:
        body: Any = json.loads(result.result)
    except ValueError as error:
        fail(f"query: QueryResult.result is not JSON: {error}")
    # `result` is a JSON string the spec does not model further; its rows are
    # the array cell under `data`.
    data: Any = body.get("data") if isinstance(body, dict) else None
    rows: Any = data.get("array_value") if isinstance(data, dict) else None
    if not isinstance(rows, list) or not rows:
        fail(
            f"query: the result has no rows under data.array_value: {result.result[:300]!r}"
        )
    row_count = len(cast(list[Any], rows))
    ok(
        f"query on {args.query_package}/{args.query_model} parsed into QueryResult with {row_count} row(s)"
    )


if __name__ == "__main__":
    main()
