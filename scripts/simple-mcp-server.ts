import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import express, { Request, Response } from 'express';
import { z } from 'zod';

// Create the MCP server instance using McpServer
const server = new McpServer({
  name: 'simple-adder-server',
  version: '1.0.0',
});

// Define the 'add' tool using the server.tool() helper
server.tool(
    'add',
    { // Pass the raw Zod shape object inline
        a: z.number().describe('The first number'),
        b: z.number().describe('The second number'),
    },
    // Destructure a and b from the handler input
    async ({ a, b }): Promise<{ content: { type: 'text', text: string }[] }> => { 
        const result = a + b; 
        return {
            content: [{ type: 'text', text: String(result) }], 
        };
    }
);

// Set up the Express app
const app = express();

let transport: SSEServerTransport | null = null;

// SSE endpoint for client connections (GET /sse)
app.get('/sse', async (req: Request, res: Response) => {
  transport = new SSEServerTransport('/messages', res);
  await server.connect(transport);
});

// Endpoint to handle client messages (POST /messages)
app.post('/messages', async (req: Request, res: Response) => {
  if (transport) {
    await transport.handlePostMessage(req, res);
  } else {
    res.status(400).send(JSON.stringify({ error: 'No active SSE connection' }));
  }
});

const port = 3002;
// Start the Express server
app.listen(port, '127.0.0.1', () => {
  console.log(`Simple MCP SSE server running with Express on http://localhost:${port}`);
}); 