from modelcontextprotocol.client import MCPClient  # external SDK

class MalloyMCPClient(MCPClient):
    """Thin wrapper that hard-codes the Malloy Publisher MCP base URL."""
    def __init__(self, host: str = "http://localhost:4040/mcp"):
        super().__init__(base_url=host) 