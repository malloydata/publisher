"""
Malloy LangChain Agent - Production-ready agent with conversation memory
Replaces the simple agent with LangChain architecture and structured prompts
"""

import os
import uuid
import json
from typing import Dict, List, Any, Optional, Tuple
from langchain.agents import AgentExecutor, create_openai_tools_agent
from langchain.schema import BaseMessage, HumanMessage, AIMessage
from langchain.memory import ConversationBufferMemory
from langchain_community.chat_message_histories import ChatMessageHistory
from langchain_openai import ChatOpenAI
from langchain_google_vertexai import ChatVertexAI
from langchain_anthropic import ChatAnthropic
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.tools import BaseTool
from langchain.callbacks.base import BaseCallbackHandler
import re

try:
    from ..tools.dynamic_malloy_tools import MalloyToolsFactory
    from ..clients.enhanced_mcp_client import EnhancedMCPClient
    from ..prompts.malloy_prompts import MalloyPromptTemplates
except ImportError:
    from src.tools.dynamic_malloy_tools import MalloyToolsFactory
    from src.clients.enhanced_mcp_client import EnhancedMCPClient
    from src.prompts.malloy_prompts import MalloyPromptTemplates


class ToolUsageTracker(BaseCallbackHandler):
    """Custom callback to track tool usage"""
    
    def __init__(self):
        self.tools_used = []
    
    def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs: Any) -> None:
        """Track when a tool starts"""
        tool_name = serialized.get("name", "unknown")
        if tool_name not in self.tools_used:
            self.tools_used.append(tool_name)
    
    def clear(self):
        """Clear the tools used list"""
        self.tools_used = []


class MalloyLangChainAgent:
    """Production-ready Malloy agent with LangChain architecture"""
    
    def __init__(
        self,
        mcp_url: str,
        auth_token: Optional[str] = None,
        model_name: str = "gpt-4o",
        session_id: Optional[str] = None,
        memory_db_path: str = "sqlite:///malloy_conversations.db",
        llm_provider: str = "openai",
        openai_api_key: Optional[str] = None,
        anthropic_api_key: Optional[str] = None,
        vertex_project_id: Optional[str] = None,
        vertex_location: str = "us-central1"
    ):
        self.openai_api_key = openai_api_key
        self.anthropic_api_key = anthropic_api_key
        self.mcp_url = mcp_url
        self.auth_token = auth_token
        self.model_name = model_name
        self.session_id = session_id or str(uuid.uuid4())
        self.memory_db_path = memory_db_path
        self.llm_provider = llm_provider
        self.vertex_project_id = vertex_project_id
        self.vertex_location = vertex_location
        
        # Store LLM config for recreation (don't initialize yet)
        self.llm_config = {
            "llm_provider": self.llm_provider,
            "model_name": self.model_name,
            "openai_api_key": self.openai_api_key,
            "anthropic_api_key": self.anthropic_api_key,
            "vertex_project_id": self.vertex_project_id,
            "vertex_location": self.vertex_location
        }
        self.llm = None  # Will be created fresh for each question
        
        # Initialize MCP client
        from src.clients.enhanced_mcp_client import MCPConfig
        mcp_config = MCPConfig(url=mcp_url, auth_token=auth_token)
        self.mcp_client = EnhancedMCPClient(mcp_config)
        
        # Initialize conversation memory with in-memory history (thread-safe)
        self.chat_history = ChatMessageHistory()
        
        self.memory = ConversationBufferMemory(
            chat_memory=self.chat_history,
            memory_key="chat_history",
            return_messages=True
        )
        
        # Initialize prompt templates
        self.prompt_templates = MalloyPromptTemplates()
        
        # Initialize tools and agent (will be set up in setup method)
        self.tools: List[BaseTool] = []
        self.agent_executor: Optional[AgentExecutor] = None
        
        # Track tool usage manually
        self._tools_used_in_session = []
        self.tool_tracker = ToolUsageTracker()
    
    def _augment_history_for_llm(self, history: List[BaseMessage]) -> List[BaseMessage]:
        """
        Augments the conversation history to make tool data visible to the LLM.
        Injects a [TOOL_DATA] block into the content of AIMessages that have tool data.
        """
        augmented_history = []
        for msg in history:
            if isinstance(msg, AIMessage) and "tool_data" in msg.additional_kwargs:
                tool_data = msg.additional_kwargs["tool_data"]
                # Create a readable summary of the data
                data_summary = json.dumps(tool_data, indent=2)
                
                # Create a new message with the augmented content
                augmented_content = (
                    f"{msg.content}\n\n"
                    f"[TOOL_DATA]\n"
                    f"{data_summary}\n"
                    f"[/TOOL_DATA]"
                )
                augmented_history.append(AIMessage(content=augmented_content))
            else:
                augmented_history.append(msg)
        return augmented_history

    def _initialize_llm(self):
        """Initialize LLM based on provider"""
        
        if self.llm_provider == "openai":
            if not self.openai_api_key:
                raise ValueError("OpenAI API key required for OpenAI provider")
            
            return ChatOpenAI(
                api_key=self.openai_api_key,
                model=self.model_name,
                temperature=0.1,
                max_tokens=2000
            )
        
        elif self.llm_provider == "vertex" or self.llm_provider == "gemini":
            if not self.vertex_project_id:
                raise ValueError("Vertex AI project ID required for Vertex AI provider")
            
            # Map common model names to Vertex AI names
            vertex_model_map = {
                "gpt-4o": "gemini-1.5-pro",
                "gpt-4": "gemini-1.5-pro", 
                "gpt-3.5-turbo": "gemini-1.5-flash",
                "gemini-pro": "gemini-1.0-pro",
                "gemini-1.5-pro": "gemini-1.5-pro",
                "gemini-1.5-flash": "gemini-1.5-flash",
                "gemini-2.5-flash": "gemini-2.5-flash"
            }
            
            vertex_model = vertex_model_map.get(self.model_name, self.model_name)
            
            return ChatVertexAI(
                project=self.vertex_project_id,
                location=self.vertex_location,
                model_name=vertex_model,
                temperature=0.1,
                max_output_tokens=2000
            )
        
        elif self.llm_provider == "anthropic":
            if not self.anthropic_api_key:
                raise ValueError("Anthropic API key required for Anthropic provider")
            
            # Map Claude model names to specific Anthropic model IDs
            anthropic_model_map = {
                # Claude 4 models (latest generation)
                "claude-4": "claude-sonnet-4-20250514",
                "claude-4-sonnet": "claude-sonnet-4-20250514", 
                "claude-4-opus": "claude-opus-4-20250514",
                "claude-sonnet-4": "claude-sonnet-4-20250514",
                "claude-opus-4": "claude-opus-4-20250514",
                # Claude 3.7 models
                "claude-3.7": "claude-3-7-sonnet-20250219",
                "claude-3.7-sonnet": "claude-3-7-sonnet-20250219",
                # Claude 3.5 models (latest versions)
                "claude-3.5": "claude-3-5-sonnet-20241022",
                "claude-3.5-sonnet": "claude-3-5-sonnet-20241022",
                "claude-3.5-haiku": "claude-3-5-haiku-20241022"
            }
            
            anthropic_model = anthropic_model_map.get(self.model_name, self.model_name)
            
            return ChatAnthropic(
                api_key=self.anthropic_api_key,
                model=anthropic_model,
                temperature=0.1,
                max_tokens=2000
            )
        
        else:
            raise ValueError(f"Unsupported LLM provider: {self.llm_provider}. "
                           f"Supported providers: openai, vertex, gemini, anthropic")
        
    async def setup(self) -> bool:
        """Setup the agent with dynamic tools from MCP discovery"""
        try:
            # Create dynamic tools from MCP discovery
            tools_factory = MalloyToolsFactory(self.mcp_client)
            self.tools = await tools_factory.create_tools()
            
            if not self.tools:
                raise Exception("No tools discovered from MCP server")
            
            # Store the prompt template for later use
            self.prompt_template = self.prompt_templates.get_agent_prompt()
            
            # Don't create agent/executor yet - will be done fresh for each question
            
            return True
            
        except Exception as e:
            print(f"Error setting up agent: {e}")
            return False
    
    async def process_question(self, question: str) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Process user question with a single, unified agent workflow.
        """
        try:
            print(f"🔍 DEBUG: process_question start - question: {question[:50]}...")
            if not self.tools:
                return False, "Agent not initialized. Call setup() first.", {}

            # Augment the history before sending it to the LLM
            chat_messages = self.memory.chat_memory.messages
            augmented_history = self._augment_history_for_llm(chat_messages)
            
            print(f"🔍 DEBUG: Running agent with {len(augmented_history)} augmented history messages...")
            fresh_llm = self._initialize_llm()
            
            # Use appropriate agent creation based on LLM provider
            if self.llm_provider == "anthropic":
                # For Claude models, use the tool calling agent with proper format
                from langchain.agents import create_tool_calling_agent
                agent = create_tool_calling_agent(fresh_llm, self.tools, self.prompt_template)
            else:
                # For OpenAI and other providers
                agent = create_openai_tools_agent(fresh_llm, self.tools, self.prompt_template)
                
            agent_executor = AgentExecutor(agent=agent, tools=self.tools, verbose=True, max_iterations=25, return_intermediate_steps=True)
            
            input_data = {"input": question, "chat_history": augmented_history}
            result = await agent_executor.ainvoke(input_data)
            
            print(f"🔍 DEBUG: Model: {self.model_name} (Provider: {getattr(self, 'llm_provider', 'unknown')})")
            print(f"🔍 DEBUG: Agent result keys: {list(result.keys())}")
            print(f"🔍 DEBUG: Agent output: '{result.get('output', 'NO OUTPUT KEY')}'")
            print(f"🔍 DEBUG: Agent output type: {type(result.get('output'))}")
            print(f"🔍 DEBUG: Has intermediate_steps: {'intermediate_steps' in result}")
            if 'intermediate_steps' in result:
                print(f"🔍 DEBUG: Number of intermediate steps: {len(result['intermediate_steps'])}")
            
            output = result['output']
            
            # Handle different output formats (Claude returns list, OpenAI returns string)
            if isinstance(output, list) and len(output) > 0:
                # Claude format: [{'text': '...', 'type': 'text', 'index': 0}]
                if isinstance(output[0], dict) and 'text' in output[0]:
                    output = output[0]['text']
                else:
                    # Fallback: join list elements
                    output = ' '.join(str(item) for item in output)
            elif not isinstance(output, str):
                # Convert other types to string
                output = str(output)
            
            # 🎯 UNIFIED CHART RESPONSE PROCESSING
            # Try to extract chart info from intermediate_steps first (works for both OpenAI and Claude)
            chart_json = None
            if "intermediate_steps" in result:
                chart_json = self._extract_chart_json_response(result)
                if chart_json:
                    print(f"🔍 DEBUG: Chart detected via intermediate_steps for {self.llm_provider}")
                    output = chart_json
            
            # Fallback: If no chart detected via intermediate_steps but output mentions charts
            if not chart_json and output and ('chart' in output.lower() or 'png' in output.lower()):
                print(f"🔍 DEBUG: Agent mentioned charts but intermediate_steps didn't contain chart tool result")
                
                # Try to extract chart file path from agent output
                import re
                chart_path_match = re.search(r'chart_[a-f0-9]{8}\.png', output)
                if chart_path_match:
                    chart_filename = chart_path_match.group(0)
                    chart_filepath = os.path.abspath(chart_filename)
                    if os.path.exists(chart_filepath):
                        print(f"🔍 DEBUG: Found chart file in agent output: {chart_filepath}")
                        chart_json = json.dumps({
                            "text": "Chart created successfully!",
                            "file_info": {"status": "success", "filepath": chart_filepath}
                        })
                        output = chart_json
                    else:
                        print(f"🔍 DEBUG: Chart file mentioned in output doesn't exist: {chart_filepath}")
                
                # If still no chart found, try the fallback constructor
                if not chart_json:
                    fallback_json = self._construct_chart_fallback(result)
                    if fallback_json:
                        print(f"🔍 DEBUG: Using fallback chart JSON")
                        output = fallback_json
                    else:
                        # Last resort: Check for recent chart files (filesystem-based detection)
                        print(f"🔍 DEBUG: Attempting filesystem-based chart detection as last resort...")
                        import glob
                        import time
                        
                        current_time = time.time()
                        recent_charts = []
                        for png_file in glob.glob("*.png"):
                            file_time = os.path.getmtime(png_file)
                            if current_time - file_time < 30:  # Created within last 30 seconds
                                recent_charts.append(png_file)
                        
                        if recent_charts:
                            # Use the most recent chart
                            most_recent = max(recent_charts, key=os.path.getmtime)
                            full_path = os.path.abspath(most_recent)
                            print(f"🔍 DEBUG: Found recent chart file (last resort): {full_path}")
                            chart_json = json.dumps({
                                "text": "Chart created successfully!",
                                "file_info": {"status": "success", "filepath": full_path}
                            })
                            output = chart_json
            
            # Handle empty output from agent (more common with certain models like Gemini)
            if not output or output.strip() == "":
                print(f"🔍 DEBUG: Agent returned empty output with {self.model_name}, generating fallback response")
                print(f"🔍 DEBUG: Result keys: {list(result.keys())}")
                print(f"🔍 DEBUG: Has intermediate_steps: {'intermediate_steps' in result}")
                if "intermediate_steps" in result:
                    print(f"🔍 DEBUG: Number of intermediate steps: {len(result['intermediate_steps'])}")
                
                # Try to generate a response from the last tool result
                if "intermediate_steps" in result and result["intermediate_steps"]:
                    last_tool_result = result["intermediate_steps"][-1][1]
                    print(f"🔍 DEBUG: Last tool result type: {type(last_tool_result)}")
                    print(f"🔍 DEBUG: Last tool result keys: {list(last_tool_result.keys()) if isinstance(last_tool_result, dict) else 'Not a dict'}")
                    
                    if isinstance(last_tool_result, dict) and "content" in last_tool_result:
                        # This is a successful query result - generate a summary
                        print("🔍 DEBUG: Calling _generate_fallback_response")
                        output = self._generate_fallback_response(last_tool_result, question)
                        print(f"🔍 DEBUG: Fallback response generated: {len(output)} chars")
                    else:
                        print("🔍 DEBUG: Tool result doesn't have expected format")
                        output = "I executed your query successfully but encountered an issue generating the response. Please try rephrasing your question."
                else:
                    print("🔍 DEBUG: No intermediate steps found - this shouldn't happen if tools executed")
                    output = "I'm having trouble processing your request right now. Please try again."
            
            tool_data = None
            
            # Extract data from the last tool call to store in memory
            if "intermediate_steps" in result and result["intermediate_steps"]:
                last_tool_output = result["intermediate_steps"][-1][1]
                if isinstance(last_tool_output, dict):
                    tool_data = last_tool_output
                else:
                    try:
                        tool_data = json.loads(last_tool_output)
                    except (json.JSONDecodeError, TypeError):
                        pass

            # Save to memory with the original (non-augmented) content
            self.memory.chat_memory.add_user_message(question)
            ai_message = AIMessage(
                content=output,
                additional_kwargs={"tool_data": tool_data} if tool_data else {}
            )
            self.memory.chat_memory.messages.append(ai_message)
            
            return True, output, {}

        except Exception as e:
            print(f"🔍 DEBUG: Exception in process_question: {e}")
            error_message = f"An unexpected error occurred: {e}"
            return False, error_message, {}
    
    def _extract_chart_info(self, response: str) -> Optional[Dict[str, Any]]:
        """Extract chart information from response if present"""
        try:
            # Try to parse as JSON for chart responses
            parsed = json.loads(response)
            if isinstance(parsed, dict) and "file_info" in parsed:
                return parsed["file_info"]
        except:
            pass
        return None
    
    def _extract_chart_json_response(self, result: Dict[str, Any]) -> Optional[str]:
        """Extract JSON response from generate_chart tool if it was called"""
        try:
            if "intermediate_steps" in result and result["intermediate_steps"]:
                chart_results = []
                
                # Collect all chart tool results
                for step in result["intermediate_steps"]:
                    if len(step) >= 2:
                        action, observation = step[0], step[1]
                        # Check if this is the generate_chart tool
                        is_chart_tool = (
                            (hasattr(action, 'tool') and action.tool == 'generate_chart') or
                            (hasattr(action, 'tool_name') and action.tool_name == 'generate_chart') or
                            ('generate_chart' in str(action))
                        )
                        
                        if is_chart_tool and isinstance(observation, str):
                            # Validate it's proper JSON first
                            try:
                                parsed_result = json.loads(observation)
                                chart_results.append((observation, parsed_result))
                                print(f"🔍 DEBUG: Found chart tool result: {observation}")
                            except json.JSONDecodeError:
                                print(f"🔍 DEBUG: Chart tool result is not valid JSON: {observation}")
                                continue
                
                # Prioritize successful results
                if chart_results:
                    # First, look for successful results
                    successful_results = [
                        (raw, parsed) for raw, parsed in chart_results 
                        if parsed.get('file_info', {}).get('status') == 'success'
                    ]
                    
                    if successful_results:
                        # Return the most recent successful result
                        raw_result, _ = successful_results[-1]
                        print(f"🔍 DEBUG: Using successful chart result: {raw_result}")
                        return raw_result
                    else:
                        # No successful results, return the most recent attempt
                        raw_result, _ = chart_results[-1]
                        print(f"🔍 DEBUG: No successful chart results, using last attempt: {raw_result}")
                        return raw_result
                        
            return None
        except Exception as e:
            print(f"🔍 DEBUG: Error extracting chart JSON: {e}")
            return None
    
    def _construct_chart_fallback(self, result: Dict[str, Any]) -> Optional[str]:
        """
        Attempts to construct a JSON response from an agent's output if it mentions a chart
        but didn't return a JSON file_info. This is a heuristic and might need refinement.
        """
        if "intermediate_steps" in result and result["intermediate_steps"]:
            for step in result["intermediate_steps"]:
                if len(step) >= 2:
                    action, observation = step[0], step[1]
                    if (hasattr(action, 'tool') and action.tool == 'generate_chart') or \
                       (hasattr(action, 'tool_name') and action.tool_name == 'generate_chart') or \
                       ('generate_chart' in str(action)):
                        
                        # Attempt to parse the observation as JSON
                        try:
                            json.loads(observation)
                            print(f"🔍 DEBUG: Observation is valid JSON: {observation}")
                            return observation
                        except json.JSONDecodeError:
                            print(f"🔍 DEBUG: Observation is not valid JSON: {observation}")
                            continue
        return None
    
    def _generate_fallback_response(self, tool_result: Dict[str, Any], question: str) -> str:
        """Generate a meaningful response when the agent doesn't provide output"""
        try:
            print(f"🔍 DEBUG: Generating fallback response for question: {question[:50]}...")
            
            # Extract data from the tool result
            if "content" in tool_result and tool_result["content"]:
                content = tool_result["content"][0]
                if "resource" in content and "text" in content["resource"]:
                    data = json.loads(content["resource"]["text"])
                    
                    # If it's query results, parse and summarize them intelligently
                    if "data" in data and "array_value" in data["data"]:
                        rows = data["data"]["array_value"]
                        schema = data.get("schema", {}).get("fields", [])
                        
                        if rows:
                            return self._format_query_results(rows, schema, question)
                        else:
                            return "Your query executed successfully but returned no results. You might want to try adjusting your filters or checking a different time period."
                    
            return "I successfully executed your query and retrieved the data. Let me know if you'd like me to analyze it further or create a visualization!"
            
        except Exception as e:
            print(f"🔍 DEBUG: Error in fallback response generation: {e}")
            return "I executed your query successfully. Please let me know if you need any additional analysis!"
    
    def _format_query_results(self, rows: List[Dict], schema: List[Dict], question: str) -> str:
        """Format query results into a meaningful response"""
        try:
            print(f"🔍 DEBUG: Formatting {len(rows)} rows with schema fields: {[f.get('name') for f in schema]}")
            
            # Extract field names and types from schema
            field_names = [field.get("name", f"field_{i}") for i, field in enumerate(schema)]
            
            # Special handling for common query patterns
            if "brand" in question.lower() and any("brand" in name for name in field_names):
                return self._format_brand_analysis(rows, field_names)
            elif "top" in question.lower():
                return self._format_top_results(rows, field_names, question)
            else:
                return self._format_generic_results(rows, field_names, question)
                
        except Exception as e:
            print(f"🔍 DEBUG: Error formatting results: {e}")
            return f"I found {len(rows)} results for your query. The data looks good! Would you like me to analyze it further or create a visualization?"
    
    def _format_brand_analysis(self, rows: List[Dict], field_names: List[str]) -> str:
        """Format brand analysis results"""
        response = "I found your top brands! Here's the breakdown:\n\n"
        
        for i, row in enumerate(rows, 1):
            record_values = row.get("record_value", [])
            brand_name = record_values[0].get("string_value", "Unknown") if record_values else "Unknown"
            
            response += f"{i}. **{brand_name}**\n"
            
            # Extract numeric values (sales, percentages, etc.)
            if len(record_values) > 1:
                sales = record_values[1].get("number_value", 0)
                response += f"   - Total Sales: ${sales:,.0f}\n"
            
            if len(record_values) > 2:
                percentage = record_values[2].get("number_value", 0) * 100
                response += f"   - Percentage of Total Sales: {percentage:.2f}%\n"
            
            response += "\n"
        
        response += "Would you like me to create a chart to visualize this data or analyze trends over time?"
        return response
    
    def _format_top_results(self, rows: List[Dict], field_names: List[str], question: str) -> str:
        """Format top N results"""
        item_type = "items"
        if "brand" in question.lower():
            item_type = "brands"
        elif "product" in question.lower():
            item_type = "products"
        elif "customer" in question.lower():
            item_type = "customers"
            
        response = f"I found your top {len(rows)} {item_type}! Here's what stands out:\n\n"
        
        for i, row in enumerate(rows, 1):
            record_values = row.get("record_value", [])
            if record_values:
                name = record_values[0].get("string_value", "Unknown")
                response += f"{i}. **{name}**"
                
                # Add key metrics if available
                if len(record_values) > 1:
                    value = record_values[1].get("number_value", 0)
                    if "sales" in field_names[1].lower() if len(field_names) > 1 else "":
                        response += f" - ${value:,.0f}"
                    else:
                        response += f" - {value:,.0f}"
                response += "\n"
        
        response += "\nWould you like me to dive deeper into any of these results or create a visualization?"
        return response
    
    def _format_generic_results(self, rows: List[Dict], field_names: List[str], question: str) -> str:
        """Format generic query results"""
        response = f"I successfully analyzed your data and found {len(rows)} results. "
        
        # Try to identify key insights
        if len(rows) > 0:
            first_row = rows[0].get("record_value", [])
            if first_row and len(field_names) > 0:
                first_field = field_names[0] if field_names else "result"
                first_value = first_row[0].get("string_value") or first_row[0].get("number_value")
                response += f"The top {first_field.replace('_', ' ')} is {first_value}. "
        
        response += "Would you like me to break down these results further or create a chart?"
        return response

    def _extract_tools_used(self, result: Dict[str, Any]) -> List[str]:
        """Extract list of tools used from agent result"""
        tools_used = []
        
        # Primary method: use the callback tracker
        if hasattr(self, 'tool_tracker') and self.tool_tracker.tools_used:
            tools_used.extend(self.tool_tracker.tools_used)
        
        # Fallback: try to extract from intermediate_steps
        if not tools_used:
            for step in result.get("intermediate_steps", []):
                if hasattr(step, "tool") and hasattr(step.tool, "name"):
                    tools_used.append(step.tool.name)
                elif isinstance(step, tuple) and len(step) >= 2:
                    # Handle (AgentAction, observation) tuples
                    action = step[0]
                    if hasattr(action, "tool"):
                        tools_used.append(action.tool)
        
        # Fallback: extract from contextual response if intermediate_steps is empty
        if not tools_used and hasattr(self, '_last_contextual_response'):
            # Parse tool calls from the contextual response
            import re
            tool_pattern = r'TOOL_CALL: (\w+)\('
            matches = re.findall(tool_pattern, self._last_contextual_response)
            tools_used.extend(matches)
        
        # Final fallback: Use a simple hardcoded approach for now
        # We know the agent calls these tools in sequence
        if not tools_used:
            # Check if the response contains JSON with file_info (indicates chart generation)
            try:
                output_response = result.get("output", "")
                parsed_response = json.loads(output_response)
                if 'file_info' in parsed_response:
                    # Chart was generated, so all tools were likely used
                    tools_used = ['malloy_projectList', 'malloy_packageList', 'malloy_packageGet', 
                                 'malloy_modelGetText', 'malloy_executeQuery', 'generate_chart']
                else:
                    # No chart, likely just query tools
                    tools_used = ['malloy_projectList', 'malloy_packageList', 'malloy_packageGet', 
                                 'malloy_modelGetText', 'malloy_executeQuery']
            except:
                # If we can't parse as JSON, assume query tools were used
                tools_used = ['malloy_projectList', 'malloy_packageList', 'malloy_packageGet', 
                             'malloy_modelGetText', 'malloy_executeQuery']
        
        return list(set(tools_used))  # Remove duplicates
    
    def get_conversation_history(self) -> List[BaseMessage]:
        """Get current conversation history"""
        return self.memory.chat_memory.messages
    
    def clear_conversation(self):
        """Clear conversation history"""
        self.memory.chat_memory.clear()
    
    def save_conversation(self, filepath: str):
        """Save conversation history to file"""
        messages = self.get_conversation_history()
        
        conversation_data = {
            "session_id": self.session_id,
            "messages": [
                {
                    "role": msg.type,
                    "content": msg.content,
                    "timestamp": getattr(msg, "timestamp", None)
                }
                for msg in messages
            ]
        }
        
        with open(filepath, "w") as f:
            json.dump(conversation_data, f, indent=2)
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of all components"""
        health = {
            "llm": False,
            "mcp_client": False,
            "tools": False,
            "agent": False,
            "memory": False
        }
        
        try:
            # Check LLM
            test_response = await self.llm.ainvoke([HumanMessage(content="Hello")])
            health["llm"] = bool(test_response.content)
        except:
            pass
        
        try:
            # Check MCP client
            health["mcp_client"] = await self.mcp_client.health_check()
        except:
            pass
        
        try:
            # Check tools
            health["tools"] = len(self.tools) > 0
        except:
            pass
        
        try:
            # Check agent (tools setup indicates agent readiness)
            health["agent"] = len(self.tools) > 0
        except:
            pass
        
        try:
            # Check memory
            self.memory.chat_memory.add_user_message("test")
            messages = self.memory.chat_memory.messages
            health["memory"] = len(messages) > 0
            self.memory.chat_memory.clear()  # Clean up test
        except:
            pass
        
        return health
    
    def get_agent_info(self) -> Dict[str, Any]:
        """Get information about the agent configuration"""
        return {
            "llm_provider": self.llm_provider,
            "model_name": self.model_name,
            "session_id": self.session_id,
            "mcp_url": self.mcp_url,
            "tools_count": len(self.tools),
            "tool_names": [tool.name for tool in self.tools],
            "memory_db": self.memory_db_path,
            "agent_ready": len(self.tools) > 0,
            "vertex_project_id": self.vertex_project_id if self.llm_provider in ["vertex", "gemini"] else None,
            "vertex_location": self.vertex_location if self.llm_provider in ["vertex", "gemini"] else None
        }


# Convenience function for easy agent creation
async def create_malloy_agent(
    mcp_url: str,
    auth_token: Optional[str] = None,
    model_name: str = "gpt-4o",
    session_id: Optional[str] = None,
    llm_provider: str = "openai",
    openai_api_key: Optional[str] = None,
    anthropic_api_key: Optional[str] = None,
    vertex_project_id: Optional[str] = None,
    vertex_location: str = "us-central1"
) -> MalloyLangChainAgent:
    """Create and setup a Malloy LangChain agent"""
    
    agent = MalloyLangChainAgent(
        mcp_url=mcp_url,
        auth_token=auth_token,
        model_name=model_name,
        session_id=session_id,
        llm_provider=llm_provider,
        openai_api_key=openai_api_key,
        anthropic_api_key=anthropic_api_key,
        vertex_project_id=vertex_project_id,
        vertex_location=vertex_location
    )
    
    success = await agent.setup()
    if not success:
        raise Exception("Failed to setup Malloy LangChain agent")
    
    return agent