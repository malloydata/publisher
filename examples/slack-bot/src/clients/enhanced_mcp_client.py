"""
Enhanced MCP Client for Malloy Publisher
- Async HTTP with aiohttp
- Dynamic tool discovery  
- Streaming support for large responses
- Authentication support
- Retry logic with exponential backoff
"""

import asyncio
import aiohttp
import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, AsyncGenerator, Union

logger = logging.getLogger(__name__)

@dataclass
class MCPConfig:
    """Configuration for MCP client"""
    url: str
    auth_token: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0

class MCPError(Exception):
    """Base exception for MCP client errors"""
    pass

class MCPConnectionError(MCPError):
    """Raised when connection to MCP server fails"""
    pass

class MCPAuthError(MCPError):
    """Raised when authentication fails"""
    pass

class MCPTimeoutError(MCPError):
    """Raised when request times out"""
    pass

class EnhancedMCPClient:
    """
    Enhanced MCP client with async support, streaming, and authentication
    """
    
    def __init__(self, config: MCPConfig):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.available_tools: Dict[str, Any] = {}
        self._request_id = 0
    
    async def __aenter__(self) -> 'EnhancedMCPClient':
        """Async context manager entry"""
        await self._create_session()
        await self.discover_tools()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
    
    async def _create_session(self):
        """Create aiohttp session with proper headers and timeout"""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json, text/event-stream'
        }
        
        # Add authentication if provided
        if self.config.auth_token:
            headers['Authorization'] = f'Bearer {self.config.auth_token}'
        
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        self.session = aiohttp.ClientSession(
            headers=headers,
            timeout=timeout,
            raise_for_status=False  # We'll handle status codes manually
        )
        
        logger.info(f"Created MCP session for {self.config.url}")
    
    async def close(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()
            self.session = None
            logger.info("Closed MCP session")
    
    def _next_request_id(self) -> int:
        """Get next request ID for JSON-RPC"""
        self._request_id += 1
        return self._request_id
    
    async def _make_request_with_retry(
        self, 
        method: str, 
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> Union[Dict[str, Any], AsyncGenerator[Dict[str, Any], None]]:
        """
        Make request with retry logic and exponential backoff
        """
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                if stream:
                    return self._stream_request(method, params)
                else:
                    return await self._make_request(method, params)
                    
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exception = e
                
                if attempt < self.config.max_retries:
                    # Exponential backoff with jitter
                    delay = min(
                        self.config.base_delay * (2 ** attempt) + random.uniform(0, 1),
                        self.config.max_delay
                    )
                    
                    logger.warning(
                        f"MCP request failed (attempt {attempt + 1}/{self.config.max_retries + 1}): {e}. "
                        f"Retrying in {delay:.2f} seconds..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"MCP request failed after {self.config.max_retries + 1} attempts")
                    
        # Convert to appropriate exception type
        if isinstance(last_exception, asyncio.TimeoutError):
            raise MCPTimeoutError(f"Request timed out after {self.config.max_retries + 1} attempts")
        elif isinstance(last_exception, aiohttp.ClientError):
            raise MCPConnectionError(f"Connection failed: {last_exception}")
        else:
            raise MCPError(f"Request failed: {last_exception}")
    
    async def _make_request(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a single JSON-RPC request"""
        if not self.session:
            raise MCPConnectionError("Session not initialized. Use async context manager.")
        
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_request_id(),
            "method": method,
            "params": params or {}
        }
        
        logger.debug(f"MCP Request: {method} with params: {params}")
        
        try:
            async with self.session.post(self.config.url, json=payload) as response:
                # Handle authentication errors
                if response.status == 401:
                    raise MCPAuthError("Authentication failed - check auth token")
                elif response.status == 403:
                    raise MCPAuthError("Access forbidden - insufficient permissions")
                
                # Handle other HTTP errors
                if response.status >= 400:
                    error_text = await response.text()
                    raise MCPConnectionError(f"HTTP {response.status}: {error_text}")
                
                response_text = await response.text()
                logger.debug(f"Raw MCP Response: {response_text}")
                
                return await self._parse_response(response_text)
                
        except aiohttp.ClientError as e:
            raise MCPConnectionError(f"Request failed: {e}")
    
    async def _stream_request(self, method: str, params: Optional[Dict[str, Any]] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Handle streaming responses for large datasets"""
        if not self.session:
            raise MCPConnectionError("Session not initialized. Use async context manager.")
        
        payload = {
            "jsonrpc": "2.0",
            "id": self._next_request_id(),
            "method": method,
            "params": params or {}
        }
        
        logger.debug(f"MCP Streaming Request: {method} with params: {params}")
        
        try:
            async with self.session.post(
                self.config.url, 
                json=payload,
                headers={'Accept': 'text/event-stream'}
            ) as response:
                
                if response.status == 401:
                    raise MCPAuthError("Authentication failed - check auth token")
                elif response.status >= 400:
                    error_text = await response.text()
                    raise MCPConnectionError(f"HTTP {response.status}: {error_text}")
                
                async for line in response.content:
                    if line.startswith(b'data: '):
                        try:
                            data = json.loads(line[6:].decode())
                            yield data
                        except json.JSONDecodeError as e:
                            logger.warning(f"Failed to parse streaming data: {e}")
                            continue
                            
        except aiohttp.ClientError as e:
            raise MCPConnectionError(f"Streaming request failed: {e}")
    
    async def _parse_response(self, response_text: str) -> Dict[str, Any]:
        """Parse MCP response, handling both regular JSON and SSE format"""
        try:
            # Handle SSE format: "event: message\ndata: {...}"
            if response_text.startswith("event: message"):
                lines = response_text.split('\n')
                for line in lines:
                    if line.startswith("data: "):
                        json_data = line[6:]  # Remove "data: " prefix
                        result = json.loads(json_data)
                        
                        if "error" in result:
                            error = result["error"]
                            raise MCPError(f"MCP Error: {error.get('message', error)}")
                        
                        return result.get("result", {})
            else:
                # Try parsing as regular JSON
                result = json.loads(response_text)
                
                if "error" in result:
                    error = result["error"]
                    raise MCPError(f"MCP Error: {error.get('message', error)}")
                
                return result.get("result", {})
                
        except json.JSONDecodeError as e:
            raise MCPError(f"Failed to parse MCP response: {e}")
    
    async def discover_tools(self) -> Dict[str, Any]:
        """
        Dynamically discover available MCP tools
        This is called automatically when entering the context manager
        """
        logger.info("Discovering available MCP tools...")
        
        try:
            result = await self._make_request_with_retry("tools/list")
            
            if isinstance(result, dict) and "tools" in result:
                self.available_tools = {
                    tool["name"]: tool for tool in result["tools"]
                }
                logger.info(f"Discovered {len(self.available_tools)} MCP tools: {list(self.available_tools.keys())}")
            else:
                logger.warning(f"Unexpected tools/list response format: {result}")
                self.available_tools = {}
            
            return self.available_tools
            
        except Exception as e:
            logger.error(f"Failed to discover tools: {e}")
            self.available_tools = {}
            return {}
    
    async def call_tool(
        self, 
        tool_name: str, 
        arguments: Dict[str, Any],
        stream: bool = False
    ) -> Union[Dict[str, Any], AsyncGenerator[Dict[str, Any], None]]:
        """
        Call a specific MCP tool
        
        Args:
            tool_name: Name of the tool to call
            arguments: Arguments to pass to the tool
            stream: Whether to use streaming response
            
        Returns:
            Tool response (dict) or async generator for streaming
        """
        if tool_name not in self.available_tools:
            available = ", ".join(self.available_tools.keys())
            raise MCPError(f"Tool '{tool_name}' not available. Available tools: {available}")
        
        params = {
            "name": tool_name,
            "arguments": arguments
        }
        
        logger.debug(f"Calling MCP tool: {tool_name} with args: {arguments}")
        
        return await self._make_request_with_retry("tools/call", params, stream=stream)
    
    async def health_check(self) -> bool:
        """
        Check if MCP server is healthy.
        This method is self-contained and creates its own session if needed.
        """
        session_was_none = self.session is None
        try:
            # If called outside of a context manager, create a temporary session.
            if session_was_none:
                await self._create_session()
            
            # A simple way to check is to discover tools.
            # This will use the session we just created.
            await self.discover_tools()
            return True
        except MCPError as e:
            logger.error(f"MCP health check failed: {e}")
            return False
        finally:
            # If we created a temporary session, clean it up.
            if session_was_none and self.session:
                await self.close()

    # Convenience methods for common Malloy operations
    # These will be implemented based on the discovered tools
    
    async def list_projects(self) -> List[str]:
        """List all available Malloy projects"""
        result = await self.call_tool("malloy_projectList", {})
        # Implementation will depend on actual MCP response format
        return self._extract_projects(result)
    
    async def list_packages(self, project_name: str) -> List[str]:
        """List packages within a project"""
        result = await self.call_tool("malloy_packageList", {"projectName": project_name})
        return self._extract_packages(result)
    
    async def get_package_contents(self, project_name: str, package_name: str) -> Dict[str, Any]:
        """Get contents of a package (models, etc.)"""
        result = await self.call_tool("malloy_packageGet", {
            "projectName": project_name,
            "packageName": package_name
        })
        return result if isinstance(result, dict) else {}
    
    async def get_model_text(self, project_name: str, package_name: str, model_path: str) -> str:
        """Get raw text content of a model file"""
        result = await self.call_tool("malloy_modelGetText", {
            "projectName": project_name,
            "packageName": package_name,
            "modelPath": model_path
        })
        return self._extract_model_text(result)
    
    async def execute_query(
        self, 
        project_name: str, 
        package_name: str, 
        model_path: str,
        query: Optional[str] = None,
        query_name: Optional[str] = None,
        source_name: Optional[str] = None,
        stream: bool = False
    ) -> Union[Dict[str, Any], AsyncGenerator[Dict[str, Any], None]]:
        """Execute a Malloy query"""
        params = {
            "projectName": project_name,
            "packageName": package_name,
            "modelPath": model_path
        }
        
        if query:
            params["query"] = query
        if query_name:
            params["queryName"] = query_name
        if source_name:
            params["sourceName"] = source_name
        
        return await self.call_tool("malloy_executeQuery", params, stream=stream)
    
    # Helper methods for parsing MCP responses
    # These will need to be implemented based on actual response formats
    
    def _extract_projects(self, result: Dict[str, Any]) -> List[str]:
        """Extract project names from MCP response"""
        projects = []
        
        # Handle the nested content/resource structure
        content = result.get("content", [])
        if not content:
            return []
        
        for item in content:
            if item.get("type") == "resource":
                resource = item.get("resource", {})
                if resource.get("type") == "application/json":
                    text = resource.get("text", "")
                    if text:
                        try:
                            # Parse the JSON string containing project data
                            project_data = json.loads(text)
                            if isinstance(project_data, list):
                                for project in project_data:
                                    if isinstance(project, dict) and "name" in project:
                                        projects.append(project["name"])
                            elif isinstance(project_data, dict) and "name" in project_data:
                                projects.append(project_data["name"])
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse project data: {e}")
                            continue
        
        return projects
    
    def _extract_packages(self, result: Dict[str, Any]) -> List[str]:
        """Extract package names from MCP response"""
        packages = []
        
        # Handle the nested content/resource structure
        content = result.get("content", [])
        if not content:
            return []
        
        for item in content:
            if item.get("type") == "resource":
                resource = item.get("resource", {})
                if resource.get("type") == "application/json":
                    text = resource.get("text", "")
                    if text:
                        try:
                            # Parse the JSON string containing package data
                            package_data = json.loads(text)
                            if isinstance(package_data, list):
                                for package in package_data:
                                    if isinstance(package, dict) and "modelPath" in package:
                                        packages.append(package["modelPath"])
                            elif isinstance(package_data, dict) and "modelPath" in package_data:
                                packages.append(package_data["modelPath"])
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse package data: {e}")
                            continue
        
        return packages
    
    def _extract_model_text(self, result: Dict[str, Any]) -> str:
        """Extract model text from MCP response"""
        # Handle the nested content/resource structure
        content = result.get("content", [])
        if not content:
            return ""
        
        for item in content:
            if item.get("type") == "resource":
                resource = item.get("resource", {})
                if resource.get("type") == "text/plain":
                    return resource.get("text", "")
        
        return ""