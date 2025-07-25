"""
Dynamic LangChain Tools from MCP Discovery
Creates LangChain tools dynamically based on MCP server capabilities
"""

from typing import List, Dict, Any, Optional, Type
from langchain.tools import BaseTool
from pydantic import BaseModel, Field, create_model
from pydantic import BaseModel as PydanticV2BaseModel
import json
import asyncio
try:
    from ..clients.enhanced_mcp_client import EnhancedMCPClient
    from ..tools.matplotlib_chart_tool import MatplotlibChartTool
except ImportError:
    from src.clients.enhanced_mcp_client import EnhancedMCPClient
    from src.tools.matplotlib_chart_tool import MatplotlibChartTool


class MalloyToolInput(BaseModel):
    """Base input model for Malloy tools"""
    pass


def create_dynamic_input_model(tool_schema: Dict[str, Any]) -> Type[BaseModel]:
    """Create a Pydantic model from MCP tool schema dynamically"""
    
    properties = tool_schema.get("inputSchema", {}).get("properties", {})
    required = tool_schema.get("inputSchema", {}).get("required", [])
    
    # Build field definitions
    field_definitions = {}
    
    for prop_name, prop_schema in properties.items():
        field_type = str  # Default to string
        field_default = ...  # Required by default
        
        # Map JSON Schema types to Python types
        if prop_schema.get("type") == "string":
            field_type = str
        elif prop_schema.get("type") == "integer":
            field_type = int
        elif prop_schema.get("type") == "boolean":
            field_type = bool
        elif prop_schema.get("type") == "array":
            field_type = List[str]  # Simplified
        
        # Set as optional if not in required list
        if prop_name not in required:
            field_type = Optional[field_type]
            field_default = None
        
        # Create field with description
        description = prop_schema.get("description", f"Parameter {prop_name}")
        field_definitions[prop_name] = (field_type, Field(default=field_default, description=description))
    
    # Create dynamic model
    model_name = f"{tool_schema['name'].title()}Input"
    return create_model(model_name, __base__=MalloyToolInput, **field_definitions)


class DynamicMalloyTool(BaseTool):
    """Dynamic LangChain tool created from MCP tool discovery"""
    
    name: str
    description: str
    args_schema: Optional[Type[BaseModel]] = None
    
    # Use model_config for Pydantic v2 compatibility
    model_config = {"arbitrary_types_allowed": True}
    
    def __init__(self, mcp_client: EnhancedMCPClient, tool_schema: Dict[str, Any], **kwargs):
        # Create dynamic input model
        input_model = create_dynamic_input_model(tool_schema)
        
        # Initialize the Pydantic model with the correct fields
        super().__init__(
            name=tool_schema["name"],
            description=tool_schema.get("description", f"Execute {tool_schema['name']} tool"),
            args_schema=input_model,
            **kwargs
        )
        
        # Store additional attributes (not part of Pydantic model)
        object.__setattr__(self, 'mcp_client', mcp_client)
        object.__setattr__(self, 'tool_name', tool_schema["name"])
        object.__setattr__(self, 'tool_schema', tool_schema)
    
    def _run(self, **kwargs: Any) -> str:
        """Execute the tool synchronously - NOT SUPPORTED, use async"""
        return f"Error: {self.tool_name} only supports async execution. Use _arun() instead."
    
    async def _arun(self, **kwargs: Any) -> str:
        """Execute the tool asynchronously"""
        try:
            print(f"🔍 DEBUG: Calling tool {self.tool_name} with {kwargs}")
            
            # Use async context manager for MCP client
            async with self.mcp_client as client:
                # Call the MCP tool
                result = await client.call_tool(self.tool_name, kwargs)
                
                # Format result more cleanly for LLM consumption
                if isinstance(result, dict):
                    # Check if this is a query result with data
                    if 'data' in result and 'isError' in result and not result.get('isError'):
                        # Clean format for successful query results
                        formatted_result = {
                            "success": True,
                            "data": result['data'],
                            "row_count": len(result['data'].get('array_value', [])) if isinstance(result['data'], dict) else 0
                        }
                        return f"QUERY_RESULT: {json.dumps(formatted_result, indent=2)}"
                    else:
                        # Regular result
                        return f"TOOL_RESULT: {json.dumps(result, indent=2)}"
                else:
                    return f"TOOL_RESULT: {str(result)}"
                
        except Exception as e:
            error_msg = f"Error executing {self.tool_name}: {str(e)}"
            print(f"🔍 DEBUG: Tool error: {error_msg}")
            return error_msg


class MalloyToolsFactory:
    """Factory for creating dynamic Malloy tools from MCP discovery"""
    
    def __init__(self, mcp_client: EnhancedMCPClient):
        self.mcp_client = mcp_client
    
    async def create_tools(self) -> List[BaseTool]:
        """Create LangChain tools dynamically from MCP server discovery"""
        
        try:
            # Use async context manager for MCP client
            async with self.mcp_client as client:
                # Discover available tools from MCP server
                tools_dict = await client.discover_tools()
                
                # Extract tool schemas from the dictionary
                tools_info = list(tools_dict.values()) if tools_dict else []
                
                langchain_tools = []
                
                for tool_schema in tools_info:
                    try:
                        # Create dynamic LangChain tool
                        tool = DynamicMalloyTool(client, tool_schema)
                        langchain_tools.append(tool)
                        
                    except Exception as e:
                        print(f"Warning: Failed to create tool {tool_schema.get('name', 'unknown')}: {e}")
                        continue
                
                # Always add the Matplotlib chart tool for chart generation
                try:
                    matplotlib_tool = MatplotlibChartTool()
                    langchain_tools.append(matplotlib_tool)
                    print(f"Added Matplotlib chart tool for chart generation")
                except Exception as e:
                    print(f"Warning: Failed to add Matplotlib chart tool: {e}")
                
                return langchain_tools
            
        except Exception as e:
            print(f"Error discovering MCP tools: {e}")
            return []
    
    def create_tools_sync(self) -> List[BaseTool]:
        """Synchronous wrapper for tool creation"""
        try:
            # Check if there's already a running event loop
            try:
                loop = asyncio.get_running_loop()
                # If we're in an async context, we need to handle this differently
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(self._run_in_new_loop)
                    return future.result()
            except RuntimeError:
                # No running loop, we can create our own
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(self.create_tools())
                finally:
                    loop.close()
        except Exception as e:
            print(f"Error in create_tools_sync: {e}")
            return []
    
    def _run_in_new_loop(self) -> List[BaseTool]:
        """Run tool creation in a new event loop (for use in thread)"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.create_tools())
        finally:
            loop.close()


# Convenience function for easy tool creation
def create_malloy_tools(mcp_url: str, auth_token: Optional[str] = None) -> List[BaseTool]:
    """Create Malloy LangChain tools from MCP server"""
    
    try:
        from ..clients.enhanced_mcp_client import MCPConfig
    except ImportError:
        from src.clients.enhanced_mcp_client import MCPConfig
    
    config = MCPConfig(url=mcp_url, auth_token=auth_token)
    client = EnhancedMCPClient(config)
    factory = MalloyToolsFactory(client)
    
    return factory.create_tools_sync()