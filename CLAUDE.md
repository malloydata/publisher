<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

See @AGENTS.md for how to run Publisher, connect an agent to the MCP endpoint, and use the bundled skills.

Publisher is a microservice: it runs locally with your own permissions, or inside your cloud behind the gateway that authenticates your users. It does not authenticate anyone, so its access controls hold only behind that gateway. See [docs/security-posture.md](docs/security-posture.md#operating-model-a-microservice-behind-your-gateway).
