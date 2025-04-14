import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { SSEClientTransport } from '@modelcontextprotocol/sdk/client/sse.js';
import { URL } from 'url';

// --- Configuration ---
const mcpServerUrl = "http://localhost:4001";

// --- Main Client Logic ---
async function runTestClient() { 
    console.log(`[CLIENT] Attempting to connect to SSE: ${mcpServerUrl}`);
    let client: Client | null = null; // Use Client type

    try {
        // 1. Create Client and Transport
        client = new Client({ // Use Client
            name: "malloy-test-client-ts", 
            version: "1.0.0" 
        });
        console.log('[CLIENT] Client created.'); // Changed log message

        
        const transport = new SSEClientTransport(new URL(mcpServerUrl)); 
        console.log(`[CLIENT] SSEClientTransport created.`);

        // 2. Connect (Initialization is likely handled internally by McpClient)
        console.log('[CLIENT] Connecting...');
        await client.connect(transport);
        console.log('[CLIENT] Connected via transport.');

        // 3. Call the 'add' tool using client.callTool with the correct object structure
        console.log(`[CLIENT] Calling method: mcp/listResources`);
        
        const listResult = await client.listResources();
        
        console.log(`[CLIENT] Method "mcp/listResources" successful! Result: ${JSON.stringify(listResult, null, 2)}`);

    } catch (error: any) {
        console.error('[CLIENT] Error:', error?.message || error);
        console.error('[CLIENT] Raw Error Details:', error);
    } finally {
        // 4. Close Client
        if (client) {
            console.log('[CLIENT] Disconnecting client...');
            await client.close().catch(e => console.error('[CLIENT] Error during close:', e)); 
            console.log('[CLIENT] Client disconnected.');
        }
    }
}

runTestClient();
