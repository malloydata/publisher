"""
Malloy LangChain Agent

A LangChain agent that can query Malloy data models and generate charts.
Now uses SimpleMCPClient which follows proper MCP SDK patterns.
"""

import json
import logging
import os
from typing import Dict, Any, List, Tuple, Optional

# Import LangChain components
from langchain_community.llms import OpenAI  # Fixed: Import from langchain-community instead of deprecated langchain.llms
from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent  # Updated: Use LangGraph for agents in LangChain 0.3.x
from langgraph.checkpoint.memory import MemorySaver  # Updated: Use LangGraph memory
from langchain.prompts import PromptTemplate
from langchain.callbacks.manager import CallbackManagerForChainRun
from langchain.schema import AgentAction, AgentFinish

from ..tools.dynamic_malloy_tools import MalloyToolsFactory
from ..prompts.malloy_prompts import MalloyPromptTemplates
from ..clients.simple_mcp_client import SimpleMCPClient


class MalloyLangChainAgent:
    """
    LangChain agent for Malloy data analysis and chart generation.
    
    Now uses SimpleMCPClient which follows proper MCP SDK patterns,
    avoiding the async context management issues we had before.
    """
    
    def __init__(
        self,
        mcp_url: str,
        model_name: str = "claude-3-5-sonnet-20241022",  # Current Claude 3.5 Sonnet v2
        llm_provider: str = "anthropic",
        session_id: str = "default",
        anthropic_api_key: Optional[str] = None,
        openai_api_key: Optional[str] = None,
        **kwargs
    ):
        self.mcp_url = mcp_url
        self.model_name = model_name
        self.llm_provider = llm_provider
        self.session_id = session_id
        self.anthropic_api_key = anthropic_api_key
        self.openai_api_key = openai_api_key  # Fixed: Store the API key
        
        # Initialize components
        self.llm = None
        self.tools = []
        self.agent = None
        self.agent_executor = None
        self.memory = None
        # Removed: self.prompt_manager = MalloyPromptTemplates()  # Not needed with LangGraph
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        
        # Create the simple MCP client
        self.mcp_client = SimpleMCPClient(mcp_url)
        
        self.logger.info(f"MalloyLangChainAgent initialized with {llm_provider} {model_name}")
    
    async def setup(self) -> bool:
        """Initialize the agent with LLM, tools, and memory"""
        try:
            self.logger.info("Setting up Malloy LangChain Agent...")
            
            # Initialize the LLM
            self._setup_llm()
            
            # Test MCP connection
            connected = await self.mcp_client.test_connection()
            if not connected:
                self.logger.error("Failed to connect to MCP server")
                return False
            
            # Create tools using the factory
            tools_factory = MalloyToolsFactory(self.mcp_url)
            self.tools = await tools_factory.create_tools()
            
            self.logger.info(f"Created {len(self.tools)} tools: {[tool.name for tool in self.tools]}")
            
            # Set up memory
            self.memory = MemorySaver()
            
            # Create the agent using LangGraph
            self._setup_agent()
            
            self.logger.info("✅ Agent setup complete")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set up agent: {e}")
            return False
    
    def _setup_llm(self):
        """Initialize the appropriate LLM"""
        if self.llm_provider == "anthropic":
            if not self.anthropic_api_key:
                raise ValueError("Anthropic API key is required for Anthropic models")
            
            self.llm = ChatAnthropic(
                model=self.model_name,
                api_key=self.anthropic_api_key,
                temperature=0.1,
                max_tokens=4000,
                timeout=120,
                max_retries=2
            )
            
        elif self.llm_provider == "openai":
            if not self.openai_api_key:
                raise ValueError("OpenAI API key is required for OpenAI models")
            
            self.llm = OpenAI(
                model_name=self.model_name,
                openai_api_key=self.openai_api_key,
                temperature=0.1,
                max_tokens=4000,
                timeout=120,
                max_retries=2
            )
        else:
            raise ValueError(f"Unsupported LLM provider: {self.llm_provider}")
        
        self.logger.info(f"Initialized {self.llm_provider} LLM: {self.model_name}")
    
    def _setup_agent(self):
        """Create the ReAct agent using LangGraph"""
        # Create the agent using LangGraph's create_react_agent
        # No need for complex prompt templates - LangGraph handles this internally
        self.agent_executor = create_react_agent(
            model=self.llm,
            tools=self.tools,
            checkpointer=self.memory  # LangGraph uses checkpointer for memory
        )
        
        self.logger.info("LangGraph agent created successfully")
    
    async def process_question(self, question: str, session_id: Optional[str] = None) -> Tuple[bool, str, Dict[str, Any]]:
        """Process a user question and return success status, response, and metadata"""
        try:
            # Use provided session_id or fall back to instance default
            effective_session_id = session_id if session_id else self.session_id
            self.logger.info(f"Processing question: {question} (session: {effective_session_id})")
            
            if not self.agent_executor:
                return False, "Agent not initialized. Please call setup() first.", {}
            
            # Use LangGraph's message-based invocation pattern
            # Each conversation needs a unique thread_id for memory
            config = {"configurable": {"thread_id": effective_session_id}}
            
            self.logger.debug("=" * 60)
            self.logger.debug("🤖 AGENT EXECUTION START")
            self.logger.debug(f"📝 User Question: {question}")
            self.logger.debug(f"🧵 Thread ID: {effective_session_id}")
            self.logger.debug(f"🔧 Available Tools: {[tool.name for tool in self.tools]}")
            self.logger.debug("=" * 60)
            
            # Execute the agent with the new message format
            result = self.agent_executor.invoke(
                {"messages": [("human", question)]},
                config
            )
            
            self.logger.debug("=" * 60)
            self.logger.debug("🎯 AGENT EXECUTION RESULT")
            self.logger.debug(f"📊 Total Messages: {len(result.get('messages', []))}")
            
            # Log all messages in the conversation
            for i, msg in enumerate(result.get("messages", [])):
                role = msg.__class__.__name__
                content = msg.content[:200] + "..." if len(msg.content) > 200 else msg.content
                self.logger.debug(f"💬 Message {i+1} ({role}): {content}")
                
                # Log tool calls if present
                if hasattr(msg, 'tool_calls') and msg.tool_calls:
                    for tool_call in msg.tool_calls:
                        self.logger.debug(f"🔧 Tool Call: {tool_call.get('name', 'unknown')} with args: {tool_call.get('args', {})}")
            
            self.logger.debug("=" * 60)
            
            # Extract the response from the last message
            if "messages" in result and result["messages"]:
                last_message = result["messages"][-1]
                response = last_message.content
                self.logger.debug(f"✅ Final Response: {response}")
            else:
                response = "No response generated"
                self.logger.warning("⚠️ No messages in result")
            
            self.logger.info(f"Agent response generated: {len(response)} chars")
            
            # Check if this looks like a chart result
            if self._extract_chart_result(response):
                self.logger.info("Detected chart generation in response")
            
            metadata = {
                "question": question,
                "session_id": self.session_id,
                "model": self.model_name,
                "provider": self.llm_provider,
                "tools_used": self._extract_tools_used(response),
                "message_count": len(result.get("messages", []))
            }
            
            return True, response, metadata
            
        except Exception as e:
            error_msg = f"Error processing question: {str(e)}"
            self.logger.error(error_msg)
            self.logger.exception("Full error details:")
            
            # Try to provide a helpful fallback response
            fallback_response = self._generate_fallback_response(question, str(e))
            
            return False, fallback_response, {"error": str(e)}
    
    def _extract_chart_result(self, response: str) -> Optional[Dict[str, Any]]:
        """Extract chart information from the response"""
        try:
            # Look for chart_url and status: success in the response
            if "chart_url" in response.lower() and "status" in response.lower():
                # Try to parse as JSON if it looks like a JSON response
                if response.strip().startswith('{') and response.strip().endswith('}'):
                    data = json.loads(response)
                    if data.get("chart_url") and data.get("status") == "success":
                        return data
                
                # Also check for chart_url in string format
                import re
                url_match = re.search(r'chart_url["\']?\s*:\s*["\']([^"\']+)["\']', response)
                if url_match:
                    return {"chart_url": url_match.group(1), "status": "success"}
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Error extracting chart result: {e}")
            return None
    
    def _extract_tools_used(self, response: str) -> List[str]:
        """Extract names of tools that were used"""
        tools_used = []
        
        # Check for chart generation
        if "chart_url" in response.lower():
            tools_used.append("generate_chart")
        
        # Check for Malloy operations
        malloy_keywords = ["malloy", "query", "project", "package", "model"]
        if any(keyword in response.lower() for keyword in malloy_keywords):
            tools_used.append("malloy_tools")
        
        return tools_used
    
    def _generate_fallback_response(self, question: str, error: str) -> str:
        """Generate a helpful fallback response when the agent fails"""
        # If the question mentions charts, try to help with chart generation
        if any(word in question.lower() for word in ["chart", "graph", "plot", "visualiz"]):
            return json.dumps({
                "text": "I encountered an error while trying to create a chart. Please try rephrasing your request or ensure you've first retrieved the data you want to visualize.",
                "error": error,
                "suggestion": "Try asking for data first, then request a chart of that specific data."
            })
        
        # General fallback
        return json.dumps({
            "text": f"I encountered an error while processing your question: {error}",
            "suggestion": "Please try rephrasing your question or check if the Malloy server is accessible."
        })
    
    def save_conversation(self, question: str, response: str, metadata: Dict[str, Any]):
        """Save conversation for debugging/analysis"""
        try:
            from datetime import datetime
            
            conversation_data = {
                "session_id": self.session_id,
                "question": question,
                "response": response,
                "metadata": metadata,
                "timestamp": datetime.now().isoformat()
            }
            
            # You could save this to a file or database
            self.logger.debug(f"Conversation saved: {conversation_data}")
            
        except Exception as e:
            self.logger.error(f"Error saving conversation: {e}")
    
    def get_conversation_history(self):
        """Get conversation history for the compatibility adapter"""
        # LangGraph manages conversation history internally through checkpoints
        # We return a minimal record to indicate this bot started/participated in the conversation
        # This enables thread continuity without duplicating LangGraph's internal memory
        
        from langchain.schema import SystemMessage
        
        return [
            SystemMessage(content=f"Bot conversation active (session: {self.session_id})")
        ]
    
    def get_agent_info(self) -> Dict[str, Any]:
        """Get information about the agent configuration"""
        return {
            "mcp_url": self.mcp_url,
            "model": self.model_name,
            "provider": self.llm_provider,
            "session_id": self.session_id,
            "tools_count": len(self.tools) if self.tools else 0,
            "tools": [tool.name for tool in self.tools] if self.tools else [],
            "status": "ready" if self.agent_executor else "not_initialized",
            "agent_type": "langgraph_react"  # Updated to reflect LangGraph usage
        }


async def create_malloy_agent(
    mcp_url: str,
    model_name: str = "claude-3-5-sonnet-20241022",  # Current Claude 3.5 Sonnet v2
    llm_provider: str = "anthropic",
    session_id: str = "default",
    **kwargs
) -> MalloyLangChainAgent:
    """Factory function to create and setup a Malloy agent"""
    agent = MalloyLangChainAgent(
        mcp_url=mcp_url,
        model_name=model_name,
        llm_provider=llm_provider,
        session_id=session_id,
        **kwargs
    )
    
    success = await agent.setup()
    if not success:
        raise RuntimeError("Failed to initialize Malloy agent")
    
    return agent


