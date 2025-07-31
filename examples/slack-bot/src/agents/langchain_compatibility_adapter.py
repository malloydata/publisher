"""
LangChain Compatibility Adapter

Bridges the gap between the synchronous Slack bot interface and the asynchronous MalloyLangChainAgent.
This adapter manages asyncio event loops, thread safety, and message serialization to make the 
async LangChain agent work seamlessly in a synchronous Slack bot environment.
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
    Synchronous wrapper for the async MalloyLangChainAgent.
    
    Handles:
    - Event loop management for async operations in sync context
    - Thread-safe execution using ThreadPoolExecutor
    - Message serialization between LangChain objects and simple dicts
    - Tool interface standardization for the Slack bot
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
            
            # Check if we're already in an async context
            try:
                # If we're in an async context, we need to run in a thread
                current_loop = asyncio.get_running_loop()
                print("🔍 DEBUG: Detected running event loop, using thread executor")
                
                def setup_in_thread():
                    # Create new loop in this thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        agent = new_loop.run_until_complete(create_malloy_agent(**self.agent_kwargs))
                        new_loop.run_until_complete(agent.setup())
                        return agent, new_loop
                    except Exception as e:
                        new_loop.close()
                        raise e
                
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(setup_in_thread)
                    self.agent, self.loop = future.result(timeout=60)
                    
            except RuntimeError:
                # No event loop running, we can create one normally
                print("🔍 DEBUG: No running event loop detected, creating new one")
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

            # Skip messages with empty content to avoid Claude API errors
            if not content or content.strip() == "":
                print(f"🔍 DEBUG: Skipping empty message with role {role}")
                continue

            if role == "user":
                deserialized.append(HumanMessage(content=content))
            elif role == "assistant":
                deserialized.append(AIMessage(content=content, additional_kwargs=kwargs))
        return deserialized

    def process_user_question(self, user_question: str, history: Optional[List[Dict[str, Any]]] = None, session_id: Optional[str] = None) -> Tuple[bool, str, List[Dict[str, Any]]]:
        try:
            self._setup_agent_if_needed()
            
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self._run_question_in_new_loop, user_question, history, session_id)
                success, response, final_history_obj = future.result(timeout=300)
                final_history = self._serialize_history(final_history_obj)
                return success, response, final_history
        except Exception as e:
            error_msg = f"Error in LangChain processing: {str(e)}"
            print(f"🔍 DEBUG: Error in process_user_question: {e}")
            return False, error_msg, []

    def _run_question_in_new_loop(self, question: str, history: Optional[List[Dict[str, Any]]], session_id: Optional[str] = None) -> Tuple[bool, str, List[BaseMessage]]:
        """This runs in a separate thread and uses the loop created during setup."""
        if not self.agent or not self.loop:
            raise RuntimeError("Adapter not initialized. Call _setup_agent_if_needed first.")
        
        # Set the event loop for this thread
        asyncio.set_event_loop(self.loop)
        
        try:
            # Note: LangGraph agents handle conversation history internally through checkpoints
            # No need to manually manage memory like with the old LangChain agents
            
            # Use provided session_id or fall back to agent's default
            effective_session_id = session_id if session_id else self.agent.session_id
            print(f"🔍 DEBUG: Using session_id: {effective_session_id} for question: {question}")
            
            success, response, _ = self.loop.run_until_complete(self.agent.process_question(question, session_id=effective_session_id))
            final_history_obj = self.agent.get_conversation_history()
            return success, response, final_history_obj
        except Exception as e:
            print(f"🔍 DEBUG: Error in _run_question_in_new_loop: {e}")
            raise e
    
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

