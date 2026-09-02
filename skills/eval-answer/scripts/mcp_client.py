#!/usr/bin/env python3
"""The one MCP tool-call client the eval scripts share. Stdlib only.

  from mcp_client import call_tool, ToolError
  payload = call_tool("http://localhost:4040/mcp", "malloy_getContext", {...})

Four scripts each carried their own copy of the JSON-RPC `tools/call` dance --
the SSE framing, the `data:` prefix, the resource envelope whose JSON is a
string inside a string -- and the fifth author rewrote it from scratch in
twenty minutes. This is that copy, once.

WHAT COMES BACK

`tools/call` answers over SSE (or plain JSON). The tool's result is a list of
content blocks; the one we want carries JSON, either as `resource.text` or as
`text`, sometimes prefixed "[Resource from publisher at <uri>]". `call_tool`
returns that JSON as a dict. A JSON-RPC error, an `isError` result, or a body
with no parseable block raises `ToolError` with what the server said, so a
dead or wrong server is a raised error and never an empty dict scored as "no
entities".

`session_headers` is for servers that hand out a session id on `initialize`
(Publisher does not require it; Credible's hosted MCP does, and also wants an
OAuth bearer -- which is why the hosted target is reached through `claude` or
the REST API rather than this client).
"""
from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from typing import Any

RESOURCE = re.compile(r"\[Resource from publisher at [^\]]+\]\s*(\{.*)", re.S)


class ToolError(RuntimeError):
    """The server answered, and the answer was an error or unparseable."""


def _parse_blocks(text: str) -> dict[str, Any]:
    """The JSON payload from an SSE or plain-JSON tools/call response."""
    lines = [l[5:].strip() if l.startswith("data:") else l.strip()
             for l in text.splitlines()]
    envelopes = []
    for line in lines:
        if line.startswith("{"):
            try:
                envelopes.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if not envelopes:
        raise ToolError(f"no JSON-RPC envelope in response: {text[:200]!r}")
    for env in envelopes:
        if "error" in env:
            raise ToolError(f"JSON-RPC error: {json.dumps(env['error'])[:300]}")
        result = env.get("result") or {}
        if result.get("isError"):
            msg = " ".join((b.get("text") or "") for b in result.get("content") or [])
            raise ToolError(f"tool error: {msg[:300]}")
        for block in result.get("content") or []:
            body = (block.get("resource") or {}).get("text") or block.get("text") or ""
            m = RESOURCE.search(body)
            cand = m.group(1) if m else body[body.find("{"):] if "{" in body else ""
            if cand:
                try:
                    return json.loads(cand)
                except json.JSONDecodeError:
                    continue
    raise ToolError(f"no parseable content block in response: {text[:200]!r}")


def call_tool(url: str, name: str, arguments: dict[str, Any], *,
              timeout: int = 120, headers: dict[str, str] | None = None
              ) -> dict[str, Any]:
    """One `tools/call`. Raises ToolError on any non-answer."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
               "params": {"name": name, "arguments": arguments}}
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            text = r.read().decode()
    except urllib.error.HTTPError as e:
        raise ToolError(f"HTTP {e.code}: {e.read().decode()[:300]}") from e
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise ToolError(f"unreachable: {e}") from e
    if not text.strip():
        # How a killed Publisher actually presents: HTTP 200 and nothing.
        raise ToolError("empty body: the server answered HTTP and returned nothing")
    return _parse_blocks(text)


def tool_ok(url: str, name: str, arguments: dict[str, Any], timeout: int = 30) -> bool:
    """True only when a real tool call returns a real payload. `/health` can
    answer while the package fails to load; this is what every stage depends on."""
    try:
        call_tool(url, name, arguments, timeout=timeout)
        return True
    except ToolError:
        return False
