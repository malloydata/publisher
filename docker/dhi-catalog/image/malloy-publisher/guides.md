## How to use this image

All examples in this guide use the public image. If you've mirrored the repository for your own use (for example, to
your Docker Hub namespace), update your commands to reference the mirrored image instead of the public one.

For example:

- Public image: `dhi.io/<repository>:<tag>`
- Mirrored image: `<your-namespace>/dhi-<repository>:<tag>`

For the examples, you must first use `docker login dhi.io` to authenticate to the registry to pull the images.

### Run a Publisher instance

Put a `publisher.config.json` in the current directory (the upstream repository's
`packages/server/publisher.config.example.duckdb.json` serves three DuckDB sample packages with no credentials)
and run:

```bash
docker run --name some-publisher -p 4000:4000 -p 4040:4040 \
  -v "$(pwd)/publisher.config.json:/publisher/publisher.config.json:ro" \
  -v publisher_data:/publisher/publisher_data \
  dhi.io/malloy-publisher:<tag>
```

The Publisher Console is at `http://localhost:4000`, the REST API under `http://localhost:4000/api/v0`, and the
MCP endpoint at `http://localhost:4040/mcp`. Poll `GET /api/v0/status` until `operationalState` is `serving`.

Without a mounted config the server still starts and serves an empty catalog; environments and packages can then
be created over the API.

### Writable paths

The server runs as `nonroot` (uid 65532). It writes `publisher.db` into `/publisher`, package clones and
per-package sandboxes into `/publisher/publisher_data`, and DuckLake data into `/publisher/ducklake_data` when a
storage destination points there. All three are owned by uid 65532 in the image, so a new named volume on any of
them is seeded writable. A bind mount must be writable by that uid; on Kubernetes set `fsGroup: 65532`.

### Configuration

Every server flag has an environment variable (`PUBLISHER_PORT`, `MCP_PORT`, `LOG_LEVEL`,
`GOOGLE_APPLICATION_CREDENTIALS`, ...); the upstream
[configuration reference](https://github.com/malloydata/publisher/blob/main/docs/configuration.md) lists them.
`PUBLISHER_NO_MCP_CONFIG=1` is set in the image so the server does not write a `.mcp.json` into its working
directory on boot.

## Non-hardened images vs Docker Hardened Images

### Key differences

| Feature         | Non-hardened Malloy Publisher       | Docker Hardened Malloy Publisher                    |
| --------------- | ----------------------------------- | --------------------------------------------------- |
| Security        | Standard base with common utilities | Minimal, hardened base with security patches        |
| Shell access    | Full shell (bash/sh) available      | No shell in runtime variants                        |
| Package manager | apt/apk available                   | No package manager in runtime variants              |
| User            | Runs as uid 1000                    | Runs as nonroot user (uid 65532)                    |
| Attack surface  | Larger due to additional utilities  | Minimal, only essential components                  |
| Debugging       | Traditional shell debugging         | Use Docker Debug or Image Mount for troubleshooting |

### Why no shell or package manager?

Docker Hardened Images prioritize security through minimalism: fewer binaries mean fewer CVEs to track and less
for an attacker to use. The binaries that are in the image are the ones the server needs: `bun`, the DuckDB CLI
(`duckdb`), `git` (for GitHub package locations), and coreutils. `docker exec <container> bun -e '<script>'`
is the general-purpose probe; for an interactive shell, use `docker debug`.
