#!/usr/bin/env python3
"""A set's run configuration, resolved in one place. Stdlib only.

A set directory may carry `eval.toml`, or `eval.json` of the same shape, naming
the servers and paths every step of a run needs:

    [model]                    # the Publisher the answerer queries
    environment = "examples"
    package     = "storefront"
    repo        = "../.."      # the model package directory
    port        = 4000         # publisher / mcp_url derive from the ports
    mcp_port    = 4040

    [truth]                    # the second server, serving the truth package
    environment = "truth"
    port        = 4881
    mcp_port    = 4882

    [paths]
    workdir = "~/.malloy-eval/storefront-tour"   # runs, packages, server roots

A value is taken from a flag first, then this file, then `set.json`, then a
built-in. Relative paths in the file resolve against the file's own directory,
so a command reads the same from any working directory.

No built-in names an example. Every script once defaulted to `--environment
samples --package ecommerce`, a set that happened to be first; on any other set
the default queried a package nobody chose and every case 404'd. A missing
environment or package is now an error that names the key to set.

Built-ins exist only for what has one right answer on a laptop: ports, the
clone this file lives in, and a work directory outside the repository. The truth
server's built-ins apply only when the set HAS a config file, because only then
did `serve.py --role truth` write that server's config from the same values;
without one, the scripts keep the truth fallbacks they had.

Guard bypasses (`--skip-golden-check`, `--no-retrieval-gate`, ...) are never read
from here. A bypass written into a file is on for every run that follows.
"""
from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

try:
    import tomllib  # Python 3.11+
except ImportError:  # pragma: no cover - exercised by patching in the tests
    tomllib = None  # type: ignore[assignment]

CLONE = pathlib.Path(__file__).resolve().parents[3]

# The one definition of what the file may hold. "path" values resolve against
# the file's directory.
SCHEMA: dict[str, dict[str, Any]] = {
    "model": {"environment": str, "package": str, "repo": "path",
              "port": int, "mcp_port": int, "publisher": str, "mcp_url": str},
    "truth": {"environment": str, "port": int, "mcp_port": int,
              "publisher": str, "package_dir": "path"},
    "paths": {"publisher_dir": "path", "skills_root": "path", "workdir": "path"},
}

BUILTIN: dict[tuple[str, str], Any] = {
    ("model", "port"): 4811,
    ("model", "mcp_port"): 4040,
    ("truth", "port"): 4881,
    ("truth", "mcp_port"): 4882,
    ("truth", "environment"): "truth",
}


class ConfigError(SystemExit):
    """A config that cannot be used. A SystemExit so a CLI prints just the text."""


class Config:
    def __init__(self, set_dir: pathlib.Path, path: pathlib.Path | None,
                 data: dict[str, dict[str, Any]], set_meta: dict[str, Any]):
        self.set_dir = set_dir
        self.path = path
        self.data = data
        self.set_meta = set_meta

    @property
    def set_name(self) -> str:
        return self.set_meta.get("name") or self.set_dir.name

    @property
    def file_hint(self) -> str:
        return str(self.path or self.set_dir / "eval.toml")

    def get(self, section: str, key: str) -> Any:
        """The file's value, else set.json's, else the built-in, else None."""
        if (section, key) not in {(s, k) for s in SCHEMA for k in SCHEMA[s]}:
            raise KeyError(f"{section}.{key} is not a config key")
        if key in self.data.get(section, {}):
            return self.data[section][key]
        if (section, key) == ("model", "package") and self.set_meta.get("targetPackage"):
            return self.set_meta["targetPackage"]
        if section == "truth" and self.path is None:
            return None
        return BUILTIN.get((section, key))

    def need(self, value: Any, section: str, key: str, flag: str) -> Any:
        """`value` if given, else the config's, else an error that says what to set."""
        if value is not None:
            return value
        got = self.get(section, key)
        if got is not None:
            return got
        what = {"model": "model", "truth": "truth-server"}.get(section, section)
        raise ConfigError(
            f"No {what} {key.replace('_', ' ')} for set '{self.set_name}'. "
            f"Fix: add `{key} = \"<value>\"` under [{section}] in "
            f"{self.file_hint}, or pass {flag}.")

    # Derived values. A URL is derived from its port unless the file names one,
    # so a port is written once and every script that needs it agrees.
    def model_publisher(self) -> str:
        return (self.get("model", "publisher")
                or f"http://localhost:{self.get('model', 'port')}")

    def model_mcp_url(self) -> str:
        return (self.get("model", "mcp_url")
                or f"http://localhost:{self.get('model', 'mcp_port')}/mcp")

    def truth_publisher(self) -> str | None:
        if self.path is None:
            return None
        return (self.get("truth", "publisher")
                or f"http://localhost:{self.get('truth', 'port')}")

    def truth_package_dir(self) -> pathlib.Path:
        return self.get("truth", "package_dir") or (self.set_dir / "truth-package")

    def workdir(self) -> pathlib.Path:
        """Where runs, built packages and server roots go: never the repository.

        A run's directory holds a `model.malloy` snapshot and a built report
        package is a Malloy package; nested inside the package under test,
        either can put that package into `loadErrors`.
        """
        return (self.get("paths", "workdir")
                or pathlib.Path.home() / ".malloy-eval" / self.set_name)

    def server_root(self, role: str) -> pathlib.Path:
        """The SERVER_ROOT `serve.py --role <role>` uses, so later steps find it."""
        return self.workdir() / "servers" / role

    def publisher_dir(self) -> pathlib.Path | None:
        given = self.get("paths", "publisher_dir")
        if given:
            return given
        here = CLONE / "packages" / "server"
        return here if (here / "dist" / "server.mjs").exists() else None

    def summary(self) -> dict[str, Any]:
        """What run.json records: the file read, and what it said."""
        return {"path": str(self.path) if self.path else None,
                "values": {s: {k: str(v) if isinstance(v, pathlib.Path) else v
                               for k, v in keys.items()}
                           for s, keys in self.data.items()}}


def _read(set_dir: pathlib.Path) -> tuple[pathlib.Path | None, dict[str, Any]]:
    toml_path, json_path = set_dir / "eval.toml", set_dir / "eval.json"
    if toml_path.exists() and json_path.exists():
        raise ConfigError(
            f"{set_dir} has both eval.toml and eval.json. Keep one; two copies "
            f"of the same settings drift apart.")
    if toml_path.exists():
        if tomllib is None:
            raise ConfigError(
                f"{toml_path} needs Python 3.11 or newer to read (this is "
                f"{sys.version.split()[0]}). Fix: run with python3.11+, or "
                f"replace it with eval.json of the same shape.")
        try:
            return toml_path, tomllib.loads(toml_path.read_text())
        except tomllib.TOMLDecodeError as e:
            raise ConfigError(f"{toml_path} is not valid TOML: {e}") from e
    if json_path.exists():
        try:
            return json_path, json.loads(json_path.read_text())
        except json.JSONDecodeError as e:
            raise ConfigError(f"{json_path} is not valid JSON: {e}") from e
    return None, {}


def _check(path: pathlib.Path, raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Validate against SCHEMA and resolve paths against the file's directory."""
    out: dict[str, dict[str, Any]] = {}
    for section, keys in raw.items():
        if section not in SCHEMA:
            raise ConfigError(
                f"Invalid {path} section [{section}]: expected one of "
                f"{', '.join(f'[{s}]' for s in SCHEMA)}.")
        if not isinstance(keys, dict):
            raise ConfigError(f"Invalid {path} on [{section}]: expected a table "
                              f"of keys, got {keys!r}.")
        out[section] = {}
        for key, value in keys.items():
            kind = SCHEMA[section].get(key)
            if kind is None:
                raise ConfigError(
                    f"Invalid {path} key '{key}' under [{section}]: expected "
                    f"one of {', '.join(SCHEMA[section])}.")
            if kind == "path":
                if not isinstance(value, str) or not value:
                    raise ConfigError(f"Invalid {path} on '{section}.{key}': "
                                      f"expected a path string, got {value!r}.")
                p = pathlib.Path(value).expanduser()
                out[section][key] = (p if p.is_absolute()
                                     else path.parent / p).resolve()
            elif kind is int:
                if isinstance(value, bool) or not isinstance(value, int):
                    raise ConfigError(f"Invalid {path} on '{section}.{key}': "
                                      f"expected an integer, got {value!r}.")
                out[section][key] = value
            else:
                if not isinstance(value, str) or not value:
                    raise ConfigError(f"Invalid {path} on '{section}.{key}': "
                                      f"expected a non-empty string, got {value!r}.")
                out[section][key] = value
    return out


def load(set_dir: pathlib.Path) -> Config:
    set_dir = pathlib.Path(set_dir).resolve()
    path, raw = _read(set_dir)
    data = _check(path, raw) if path else {}
    meta_path = set_dir / "set.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    return Config(set_dir, path, data, meta)
