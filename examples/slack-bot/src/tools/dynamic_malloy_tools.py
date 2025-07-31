"""
Dynamic Malloy Tools Factory

Creates LangChain tools dynamically from MCP server capabilities.
Now uses SimpleMCPClient which follows proper MCP SDK patterns.
"""

import json
import logging
from typing import Dict, Any, List, Type

from langchain.tools import BaseTool
from pydantic import BaseModel, Field, create_model

from ..clients.simple_mcp_client import SimpleMCPClient
from ..tools.quickchart_tool import QuickChartTool


def create_pydantic_schema_from_mcp(tool_name: str, input_schema: Dict[str, Any]) -> Type[BaseModel]:
    """Create a Pydantic schema from MCP tool input schema"""
    properties = input_schema.get("properties", {})
    required_fields = input_schema.get("required", [])
    
    # Create field definitions for Pydantic
    field_definitions = {}
    
    for field_name, field_info in properties.items():
        field_type = str  # Default to string
        default_value = ...  # Required by default
        description = field_info.get("description", "")
        
        # Convert JSON Schema types to Python types
        json_type = field_info.get("type", "string")
        if json_type == "string":
            field_type = str
        elif json_type == "integer":
            field_type = int
        elif json_type == "boolean":
            field_type = bool
        
        # Set default value if field is not required
        if field_name not in required_fields:
            if json_type == "string":
                default_value = field_info.get("default", "")
            elif json_type == "integer":
                default_value = field_info.get("default", 0)
            elif json_type == "boolean":
                default_value = field_info.get("default", False)
        
        # Create the field definition
        if default_value is ...:
            field_definitions[field_name] = (field_type, Field(description=description))
        else:
            field_definitions[field_name] = (field_type, Field(default=default_value, description=description))
    
    # Create the dynamic model
    model_name = f"{tool_name}Input"
    return create_model(model_name, **field_definitions)


class DynamicMalloyTool(BaseTool):
    """A LangChain tool that wraps MCP Malloy operations"""
    
    def __init__(self, mcp_client: SimpleMCPClient, tool_name: str, description: str, args_schema: Type[BaseModel]):
        # Call super with required fields first
        super().__init__(
            name=tool_name,
            description=description,
            args_schema=args_schema
        )
        
        # Store these as instance variables after super init
        self._mcp_client = mcp_client
        self._logger = logging.getLogger(__name__)

    def _run(self, **kwargs) -> str:
        """Synchronous wrapper that runs the async implementation"""
        import asyncio
        
        # Get the current event loop or create a new one
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If there's already a running loop, we need to use a new thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, self._arun(**kwargs))
                    return future.result()
            else:
                # No running loop, we can use it directly
                return loop.run_until_complete(self._arun(**kwargs))
        except RuntimeError:
            # No event loop, create a new one
            return asyncio.run(self._arun(**kwargs))

    async def _arun(
        self,
        **kwargs
    ) -> str:
        """Execute the Malloy tool operation dynamically"""
        import json  # Local import to ensure availability in async context
        
        try:
            self._logger.debug("=" * 50)
            self._logger.debug(f"🛠️ TOOL EXECUTION: {self.name}")
            self._logger.debug(f"📝 Arguments: {kwargs}")
            self._logger.debug("=" * 50)
            
            # Execute the tool dynamically via MCP client
            self._logger.debug(f"🔧 Executing {self.name} with MCP client")
            result = await self._mcp_client.call_tool(self.name, kwargs)
            
            self._logger.debug(f"✅ Tool result: {result}")
            return json.dumps(result)
                
        except Exception as e:
            self._logger.error(f"❌ Error executing {self.name}: {e}")
            return json.dumps({
                "operation": self.name.replace("malloy_", ""),
                "success": False,
                "error": str(e),
                "tool_name": self.name,
                "arguments": kwargs
            })


class MalloyToolsFactory:
    """Factory for creating Malloy tools from MCP server capabilities"""
    
    def __init__(self, mcp_url: str):
        self.mcp_url = mcp_url
        self.mcp_client = SimpleMCPClient(mcp_url)
        self.logger = logging.getLogger(__name__)

    async def create_tools(self) -> List[BaseTool]:
        """Create all available tools dynamically from MCP server definitions"""
        try:
            self.logger.info("Creating Malloy tools using SimpleMCPClient")
            
            # Test connection first
            connected = await self.mcp_client.test_connection()
            if not connected:
                self.logger.error("Failed to connect to MCP server")
                return [QuickChartTool()]
            
            # Get tool definitions from MCP server
            tool_definitions = await self.mcp_client.get_tool_definitions()
            self.logger.debug(f"Retrieved {len(tool_definitions)} tool definitions from MCP server")
            
            malloy_tools = []
            
            # Create tools dynamically from MCP definitions
            for tool_def in tool_definitions:
                tool_name = tool_def["name"]
                description = tool_def["description"]
                input_schema = tool_def["inputSchema"]
                
                self.logger.debug(f"Creating tool: {tool_name}")
                self.logger.debug(f"  Schema: {input_schema}")
                
                # Create dynamic Pydantic schema
                try:
                    pydantic_schema = create_pydantic_schema_from_mcp(tool_name, input_schema)
                    
                    # Create the tool with dynamic schema
                    tool = DynamicMalloyTool(
                        self.mcp_client,
                        tool_name,
                        description,
                        pydantic_schema
                    )
                    malloy_tools.append(tool)
                    
                    self.logger.debug(f"✅ Created tool: {tool_name}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to create tool {tool_name}: {e}")
                    continue
            
            # Add the chart generation tool
            chart_tool = QuickChartTool()
            
            all_tools = malloy_tools + [chart_tool]
            
            self.logger.info(f"Created {len(all_tools)} tools: {[tool.name for tool in all_tools]}")
            return all_tools
            
        except Exception as e:
            self.logger.error(f"Error creating Malloy tools: {e}")
            self.logger.exception("Full error details:")
            # Return at least the chart tool if Malloy tools fail
            return [QuickChartTool()]

