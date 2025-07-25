"""
LangChain Compatibility Adapter
Provides a drop-in replacement for SimpleMalloyAgent using LangChain architecture
"""

import json
import asyncio
import threading
from typing import Dict, List, Any, Optional, Tuple
from .malloy_langchain_agent import MalloyLangChainAgent
from langchain.schema import HumanMessage, AIMessage, BaseMessage
from ..agents.malloy_langchain_agent import MalloyLangChainAgent, create_malloy_agent
from concurrent.futures import ThreadPoolExecutor


class LangChainCompatibilityAdapter:
    """
    Adapter to make the async MalloyLangChainAgent compatible with a sync bot interface.
    It handles the asyncio event loop management.
    """
    def __init__(self, **kwargs):
        self.agent_kwargs = kwargs
        # The agent is created just-in-time to ensure it's in the right thread context.
        self.agent: Optional[MalloyLangChainAgent] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        print("🔍 DEBUG: LangChainCompatibilityAdapter initialized.")

    def _setup_agent_if_needed(self):
        """Initializes the agent and its setup in a new event loop if not already done."""
        if self.agent is None:
            print("🔍 DEBUG: First-time setup for agent in adapter.")
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            self.agent = self.loop.run_until_complete(create_malloy_agent(**self.agent_kwargs))
            self.loop.run_until_complete(self.agent.setup())
            print("🔍 DEBUG: Agent setup complete in adapter.")

    def _serialize_history(self, messages: List[BaseMessage]) -> List[Dict[str, Any]]:
        serialized = []
        for msg in messages:
            if isinstance(msg, HumanMessage):
                role = "user"
            elif isinstance(msg, AIMessage):
                role = "assistant"
            else:
                role = "system"
            
            # Preserve the tool_data if it exists
            content = {
                "content": msg.content,
                "additional_kwargs": msg.additional_kwargs
            }
            serialized.append({"role": role, "content": content})
        return serialized

    def _deserialize_history(self, history: List[Dict[str, Any]]) -> List[BaseMessage]:
        deserialized = []
        for msg in history:
            role = msg.get("role")
            content_data = msg.get("content", {})
            content = content_data.get("content", "")
            kwargs = content_data.get("additional_kwargs", {})

            if role == "user":
                deserialized.append(HumanMessage(content=content))
            elif role == "assistant":
                deserialized.append(AIMessage(content=content, additional_kwargs=kwargs))
        return deserialized

    def process_user_question(self, user_question: str, history: Optional[List[Dict[str, Any]]] = None) -> Tuple[bool, str, List[Dict[str, Any]]]:
        try:
            self._setup_agent_if_needed()
            
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._run_question_in_new_loop, user_question, history)
                success, response, final_history_obj = future.result(timeout=300)
                final_history = self._serialize_history(final_history_obj)
                return success, response, final_history
        except Exception as e:
            error_msg = f"Error in LangChain processing: {str(e)}"
            print(f"🔍 DEBUG: Error in process_user_question: {e}")
            return False, error_msg, []

    def _run_question_in_new_loop(self, question: str, history: Optional[List[Dict[str, Any]]]) -> Tuple[bool, str, List[BaseMessage]]:
        """This runs in a separate thread and uses the loop created during setup."""
        if not self.agent or not self.loop:
            raise RuntimeError("Adapter not initialized. Call _setup_agent_if_needed first.")
        
        asyncio.set_event_loop(self.loop)
        
        if history:
            deserialized_history = self._deserialize_history(history)
            self.agent.memory.chat_memory.messages = deserialized_history

        success, response, _ = self.loop.run_until_complete(self.agent.process_question(question))
        final_history_obj = self.agent.get_conversation_history()
        return success, response, final_history_obj
            
    def _convert_history_to_simple_format(self) -> List[Dict[str, Any]]:
        """Convert LangChain message history to SimpleMalloyAgent format"""
        
        try:
            langchain_messages = self.agent.get_conversation_history()
            converted_messages = []
            
            for msg in langchain_messages:
                if hasattr(msg, 'type') and hasattr(msg, 'content'):
                    # Map LangChain message types to OpenAI format
                    if msg.type == 'human':
                        role = 'user'
                    elif msg.type == 'ai':
                        role = 'assistant'
                    elif msg.type == 'system':
                        role = 'system'
                    else:
                        role = 'assistant'  # Default fallback
                    
                    message_dict = {
                        "role": role,
                        "content": msg.content
                    }
                    
                    # Add additional fields if they exist
                    if hasattr(msg, 'additional_kwargs'):
                        message_dict.update(msg.additional_kwargs)
                    
                    converted_messages.append(message_dict)
            
            return converted_messages
            
        except Exception as e:
            print(f"Error converting history: {e}")
            return []
    
    def get_available_tools(self) -> List[Dict[str, Any]]:
        """Get available tools in OpenAI function format - for compatibility"""
        
        if not self.agent:
            return []
        
        try:
            # Convert LangChain tools to OpenAI function format
            openai_tools = []
            
            for tool in self.agent.tools:
                # Extract schema from LangChain tool
                tool_schema = {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                }
                
                # Try to extract parameters from args_schema
                if hasattr(tool, 'args_schema') and tool.args_schema:
                    schema = tool.args_schema.schema()
                    if 'properties' in schema:
                        tool_schema["parameters"]["properties"] = schema['properties']
                    if 'required' in schema:
                        tool_schema["parameters"]["required"] = schema['required']
                
                openai_tools.append(tool_schema)
            
            return openai_tools
            
        except Exception as e:
            print(f"Error getting available tools: {e}")
            return []
    
    def call_tool(self, tool_name: str, **kwargs) -> str:
        """Call a tool directly - for compatibility"""
        
        if not self.agent:
            return f"Error: LangChain agent not initialized"
        
        try:
            # Find the tool
            tool = None
            for t in self.agent.tools:
                if t.name == tool_name:
                    tool = t
                    break
            
            if not tool:
                return f"Error: Tool '{tool_name}' not found"
            
            # Call the tool
            result = tool.run(**kwargs)
            
            # Format result as JSON string for compatibility
            if isinstance(result, dict):
                return json.dumps(result, indent=2)
            else:
                return str(result)
                
        except Exception as e:
            return f"Error calling tool {tool_name}: {str(e)}"
    
    def health_check(self) -> bool:
        """Check if the agent is healthy"""
        try:
            self._setup_agent_if_needed()
            if not self.agent:
                return False
            
            # This needs to run in the agent's event loop
            if not self.loop or self.loop.is_closed():
                raise RuntimeError("Event loop not available for health check.")

            health_info = self.loop.run_until_complete(self.agent.health_check())
            healthy_components = sum(1 for status in health_info.values() if status)
            return healthy_components > 0
        except Exception as e:
            print(f"Health check failed with an exception: {e}")
            return False
    
    def get_agent_info(self) -> Dict[str, Any]:
        """Get information about the agent's configuration"""
        
        base_info = {
            "adapter_type": "LangChain",
            "llm_provider": self.agent_kwargs.get("llm_provider", "unknown"),
            "llm_model": self.agent_kwargs.get("llm_model", "unknown"),
            "mcp_url": self.agent_kwargs.get("mcp_url", "unknown"),
            "setup_complete": self.agent is not None,
            "vertex_project_id": self.agent_kwargs.get("vertex_project_id", None),
            "vertex_location": self.agent_kwargs.get("vertex_location", None)
        }
        
        if self.agent:
            try:
                langchain_info = self.agent.get_agent_info()
                base_info.update(langchain_info)
            except Exception as e:
                base_info["error"] = str(e)
        
        return base_info
    
    def clear_conversation(self):
        """Clear conversation history"""
        if self.agent:
            try:
                self.agent.clear_conversation()
            except Exception as e:
                print(f"Error clearing conversation: {e}")
    
    def save_conversation(self, filepath: str):
        """Save conversation history to file"""
        if self.agent:
            try:
                self.agent.save_conversation(filepath)
            except Exception as e:
                print(f"Error saving conversation: {e}")
    
    # Properties for compatibility
    @property
    def mcp_client(self):
        """Provide access to MCP client for compatibility"""
        if self.agent:
            return self.agent.mcp_client
        return None


# Factory function for easy replacement
def create_compatible_agent(openai_api_key: str = None, mcp_url: str = "http://localhost:4040/mcp", 
                           llm_provider: str = "openai", llm_model: str = "gpt-4o", 
                           anthropic_api_key: Optional[str] = None,
                           vertex_project_id: Optional[str] = None, 
                           vertex_location: str = "us-central1") -> LangChainCompatibilityAdapter:
    """
    Create a LangChain-powered agent that's compatible with SimpleMalloyAgent
    This is a drop-in replacement that can be used anywhere SimpleMalloyAgent is used
    """
    
    return LangChainCompatibilityAdapter(
        openai_api_key=openai_api_key,
        mcp_url=mcp_url,
        llm_provider=llm_provider,
        llm_model=llm_model,
        anthropic_api_key=anthropic_api_key,
        vertex_project_id=vertex_project_id,
        vertex_location=vertex_location
    )