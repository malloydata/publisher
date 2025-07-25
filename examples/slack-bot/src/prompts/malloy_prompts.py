"""
Simplified Malloy Prompt Templates
Natural prompts that allow LLM to think and reason freely
"""

from typing import Dict, Any, Optional
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder


class MalloyPromptTemplates:
    """Simplified prompt templates for Malloy operations"""
    
    def __init__(self, version: str = "v2.0"):
        self.version = version
    
    def get_agent_prompt(self) -> ChatPromptTemplate:
        """Simple, natural agent prompt that allows free thinking"""
        
        system_message = """You are a helpful data analyst with access to tools for exploring and analyzing data.

You can help users:
- Explore available data projects and models
- Run queries to analyze data
- Create visualizations and charts
- Answer questions about data insights

Use the available tools naturally to help users with their data questions. Think step by step and explain your findings in a friendly, conversational way.

When users ask for charts or visualizations, get the data first, then create an appropriate chart using the chart generation tool.

If something doesn't work, try a different approach or ask for clarification. Be helpful and adaptive."""

        return ChatPromptTemplate.from_messages([
            ("system", system_message),
            MessagesPlaceholder(variable_name="chat_history", optional=True),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad")
        ])
    
    def get_prompt_version_info(self) -> Dict[str, Any]:
        """Get information about current prompt version"""
        return {
            "version": self.version,
            "description": "Simplified, natural prompts for better LLM reasoning",
            "complexity": "minimal"
        }