"""
Simple MCP Client following official SDK patterns.

This client follows the recommended patterns from the MCP Python SDK documentation,
creating new sessions per operation rather than trying to reuse them.
"""
import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


class SimpleMCPClient:
    """
    Simple MCP client that follows official SDK patterns.
    
    Creates new client sessions per operation as recommended by the MCP SDK.
    This avoids the async context management issues we were having with
    trying to reuse sessions across different async contexts.
    """
    
    def __init__(self, mcp_url: str):
        """Initialize with MCP server URL."""
        self.mcp_url = mcp_url
        self.logger = logging.getLogger(__name__)
        
        # Parse URL to validate
        parsed = urlparse(mcp_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid MCP URL: {mcp_url}")
            
        self.logger.debug(f"SimpleMCPClient initialized for {mcp_url}")
    
    async def list_projects(self) -> List[Dict[str, Any]]:
        """List all available projects."""
        self.logger.debug("🌐 MCP: Listing projects")
        
        async with streamablehttp_client(self.mcp_url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                self.logger.debug("🤝 MCP: Session initialized successfully")
                
                # Call the list projects tool - no parameters needed
                self.logger.debug("🔧 MCP: Calling malloy_projectList tool")
                result = await session.call_tool("malloy_projectList", {})
                self.logger.debug(f"📤 MCP: Tool call result type: {type(result)}")
                self.logger.debug(f"📤 MCP: Tool call result content: {result}")
                
                # Parse the result
                if result.content and len(result.content) > 0:
                    import json
                    try:
                        content = result.content[0]
                        self.logger.debug(f"📋 MCP: First content item: {content}")
                        
                        # Handle EmbeddedResource format
                        text_content = None
                        if hasattr(content, 'resource') and hasattr(content.resource, 'text'):
                            text_content = content.resource.text
                            self.logger.debug(f"📝 MCP: Resource text: {text_content}")
                        elif hasattr(content, 'text'):
                            text_content = content.text
                            self.logger.debug(f"📝 MCP: Content text: {text_content}")
                        
                        if text_content:
                            data = json.loads(text_content)
                            
                            # Handle different response formats
                            if isinstance(data, list):
                                # Server returns projects directly as an array
                                projects = data
                            else:
                                # Server returns projects wrapped in an object
                                projects = data.get('projects', [])
                                
                            self.logger.debug(f"✅ MCP: Successfully parsed {len(projects)} projects")
                            return projects
                        else:
                            self.logger.warning("⚠️ MCP: No text content found in response")
                            
                    except (json.JSONDecodeError, AttributeError) as e:
                        self.logger.error(f"❌ MCP: Error parsing projects result: {e}")
                        self.logger.debug(f"❌ MCP: Content structure: {content}")
                        
                self.logger.warning("⚠️ MCP: No valid content found in result")
                return []
    
    async def list_packages(self, project_name: str) -> List[Dict[str, Any]]:
        """List packages for a given project."""
        self.logger.debug(f"Listing packages for project: {project_name}")
        
        async with streamablehttp_client(self.mcp_url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                result = await session.call_tool("malloy_packageList", {"projectName": project_name})
                
                if result.content and len(result.content) > 0:
                    import json
                    try:
                        content = result.content[0]
                        if hasattr(content, 'text'):
                            data = json.loads(content.text)
                            return data.get('packages', [])
                    except (json.JSONDecodeError, AttributeError) as e:
                        self.logger.error(f"Error parsing packages result: {e}")
                        
                return []
    
    async def get_package(self, project_name: str, package_name: str) -> Dict[str, Any]:
        """Get package details including models."""
        self.logger.debug(f"Getting package: {project_name}/{package_name}")
        
        async with streamablehttp_client(self.mcp_url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                result = await session.call_tool("malloy_packageGet", {
                    "projectName": project_name,
                    "packageName": package_name
                })
                
                if result.content and len(result.content) > 0:
                    import json
                    try:
                        content = result.content[0]
                        if hasattr(content, 'text'):
                            return json.loads(content.text)
                    except (json.JSONDecodeError, AttributeError) as e:
                        self.logger.error(f"Error parsing package result: {e}")
                        
                return {}
    
    async def get_model_text(self, project_name: str, package_name: str, model_path: str) -> str:
        """Get the raw text content of a model file."""
        self.logger.debug(f"Getting model text: {project_name}/{package_name}/{model_path}")
        
        async with streamablehttp_client(self.mcp_url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                result = await session.call_tool("malloy_modelGetText", {
                    "projectName": project_name,
                    "packageName": package_name,
                    "modelPath": model_path
                })
                
                if result.content and len(result.content) > 0:
                    import json
                    try:
                        content = result.content[0]
                        if hasattr(content, 'text'):
                            data = json.loads(content.text)
                            return data.get('content', '')
                    except (json.JSONDecodeError, AttributeError) as e:
                        self.logger.error(f"Error parsing model text result: {e}")
                        
                return ""
    
    async def execute_query(
        self, 
        project_name: str, 
        package_name: str, 
        model_path: str, 
        query: Optional[str] = None,
        query_name: Optional[str] = None,
        source_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Execute a Malloy query."""
        self.logger.debug(f"Executing query on {project_name}/{package_name}/{model_path}")
        
        async with streamablehttp_client(self.mcp_url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # Prepare arguments
                args = {
                    "projectName": project_name,
                    "packageName": package_name,
                    "modelPath": model_path
                }
                
                if query:
                    args["query"] = query
                if query_name:
                    args["queryName"] = query_name
                if source_name:
                    args["sourceName"] = source_name
                
                result = await session.call_tool("malloy_executeQuery", args)
                
                if result.content and len(result.content) > 0:
                    import json
                    try:
                        content = result.content[0]
                        if hasattr(content, 'text'):
                            return json.loads(content.text)
                    except (json.JSONDecodeError, AttributeError) as e:
                        self.logger.error(f"Error parsing query result: {e}")
                        
                return {}
    
    async def test_connection(self) -> bool:
        """Test if we can connect to the MCP server."""
        try:
            self.logger.debug(f"🌐 MCP: Testing connection to {self.mcp_url}")
            
            async with streamablehttp_client(self.mcp_url) as (read, write, _):
                async with ClientSession(read, write) as session:
                    self.logger.debug("🤝 MCP: Initializing session...")
                    await session.initialize()
                    self.logger.debug("✅ MCP: Session initialized successfully")
                    
                    # Try to list tools to verify connection
                    self.logger.debug("🔧 MCP: Listing available tools...")
                    tools = await session.list_tools()
                    self.logger.debug(f"📋 MCP: Found {len(tools.tools)} tools:")
                    for tool in tools.tools:
                        self.logger.debug(f"  🔨 {tool.name}: {tool.description}")
                    
                    self.logger.debug(f"✅ MCP: Connected successfully, found {len(tools.tools)} tools")
                    return True
                    
        except Exception as e:
            self.logger.error(f"❌ MCP: Failed to connect to MCP server: {e}")
            self.logger.exception("Full connection error:")
            return False

    async def get_tool_definitions(self) -> List[Dict[str, Any]]:
        """Get tool definitions from the MCP server."""
        self.logger.debug("🔧 MCP: Getting tool definitions")
        
        async with streamablehttp_client(self.mcp_url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                # Get tool definitions
                tools_response = await session.list_tools()
                tool_definitions = []
                
                for tool in tools_response.tools:
                    tool_def = {
                        "name": tool.name,
                        "description": tool.description,
                        "inputSchema": tool.inputSchema if tool.inputSchema else {}
                    }
                    tool_definitions.append(tool_def)
                    self.logger.debug(f"🔨 Tool: {tool.name}")
                    self.logger.debug(f"   Schema: {tool_def['inputSchema']}")
                
                self.logger.debug(f"✅ MCP: Retrieved {len(tool_definitions)} tool definitions")
                return tool_definitions

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute any MCP tool dynamically with the given arguments."""
        self.logger.debug(f"🔧 Calling MCP tool: {tool_name} with args: {arguments}")
        
        async with streamablehttp_client(self.mcp_url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                
                try:
                    # Call the tool with the provided arguments
                    result = await session.call_tool(tool_name, arguments)
                    self.logger.debug(f"✅ MCP tool {tool_name} result: {result}")
                    
                    # Parse the result - handle different response formats
                    if hasattr(result, 'content') and result.content:
                        content = result.content[0]
                        if hasattr(content, 'resource') and hasattr(content.resource, 'text'):
                            # Handle resource responses with JSON text
                            try:
                                return json.loads(content.resource.text)
                            except json.JSONDecodeError:
                                return {"raw_text": content.resource.text}
                        elif hasattr(content, 'text'):
                            # Handle direct text responses
                            try:
                                return json.loads(content.text)
                            except json.JSONDecodeError:
                                return {"raw_text": content.text}
                    
                    # Fallback - return the raw result
                    return {"raw_result": str(result)}
                    
                except Exception as e:
                    self.logger.error(f"❌ Error calling tool {tool_name}: {e}")
                    return {
                        "error": str(e),
                        "tool_name": tool_name,
                        "arguments": arguments,
                        "success": False
                    }

