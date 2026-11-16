<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

See @AGENTS.md for how to run Publisher, connect an agent to the MCP endpoint, and use the bundled skills.

Publisher is a microservice: it runs locally with your own permissions, or inside your cloud behind the gateway that authenticates your users. It does not authenticate anyone, so its access controls hold only behind that gateway. See [docs/security-posture.md](docs/security-posture.md#operating-model-a-microservice-behind-your-gateway).

## Adding or changing a setting

Every server setting is one entry in the settings registry, `packages/server/src/settings/registry/`. That covers each environment variable, command-line flag and `publisher.config.json` key. Read a setting with `settings.get("<key>")`. Never read `process.env`, `process.argv` or the config file for one: ESLint fails a `process.env` read anywhere in `packages/server/src` outside `src/settings`, the specs and the test helpers.

- **The key gives every name.** The variable is `PUBLISHER_` plus the key in upper snake case. The flag is `--` plus the key in kebab case, and a boolean also gets `--no-` to turn it off. The file key is the key. Don't choose a name by hand. `aliases` is only for a name that has already shipped in a release: it keeps working, deprecated, with a warning.
- **Launch settings** (`kind: "launch"`: `root`, `config`, `useBundledDefault`, `initializeStorage`, `watchEnvironments`) decide which file is read, so they are a variable and a flag only. Startup ignores one found in the file, and says so.
- **A secret** (`secret: true`) has no flag and is never logged. In the file it must be a whole `${VAR}` reference.
- **A structured setting** (`kind: "structured"`, the `retrieval` block) is validated as a whole, and each of its leaves is also a setting with its own variable and flag.
- **A variable Publisher reads but does not own** (`NODE_ENV`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `GOOGLE_APPLICATION_CREDENTIALS`, the `PG*` ones) stays outside the registry. It goes in `ExternalVariable` in `src/settings/external.ts` and is read with `externalEnv()`. A `${VAR}` the config file names is content, not a setting, and is read with `referencedVariable()`.

After adding or changing one:

- Run `bun run generate-settings-reference` in `packages/server`. It rewrites the table in `docs/configuration.md` and `publisher.config.json.example`, and `reference.spec.ts` fails while either is stale.
- Add a flag that takes a value to `SERVER_VALUE_FLAGS` in `packages/create-malloy-package/src/scaffold.ts`. A drift test fails otherwise.
- Write a `RELEASE_NOTES.md` section when users will see the change.
- A test harness or script that starts this tree's server sets the derived names. One that also starts an older build, such as an A/B baseline, keeps the old names, which every release still reads.

The design, and which old name maps to which key, is [docs/configuration-design.md](docs/configuration-design.md).
