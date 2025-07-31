"""
Test prompt templates functionality
"""

import pytest
from src.prompts.malloy_prompts import MalloyPromptTemplates


class TestMalloyPromptTemplates:
    """Test prompt template generation"""
    
    def setup_method(self):
        """Setup test environment"""
        self.prompt_templates = MalloyPromptTemplates()
    
    def test_init(self):
        """Test prompt templates initialization"""
        assert self.prompt_templates.version == "v2.0"
    
    def test_get_agent_prompt(self):
        """Test main agent prompt generation"""
        prompt = self.prompt_templates.get_agent_prompt()
        
        # Check that we get a ChatPromptTemplate
        assert prompt is not None
        assert hasattr(prompt, 'messages')
        
        # Should have system message, chat history, human input, and agent scratchpad
        assert len(prompt.messages) == 4
    
    def test_simplified_approach(self):
        """Test that the new simplified prompts are much shorter"""
        prompt = self.prompt_templates.get_agent_prompt()
        
        # Get the system message content
        system_message = prompt.messages[0]
        system_content = system_message.prompt.template
        
        # Should be much shorter than the old 391-line version
        assert len(system_content) < 1000, "Prompt should be simplified"
        
        # Should contain key concepts but not be overly prescriptive
        assert "data analyst" in system_content.lower()
        assert "tools" in system_content.lower() 
        assert "helpful" in system_content.lower()
        
        # Should NOT contain complex instructions
        assert "SYNTAX RULES" not in system_content
        assert "CRITICAL:" not in system_content
        assert "📋" not in system_content  # No emoji sections
    
    def test_version_info(self):
        """Test version information"""
        info = self.prompt_templates.get_prompt_version_info()
        
        assert info["version"] == "v2.0"
        assert info["description"]
        assert info["complexity"] == "minimal"
    
    def test_natural_language(self):
        """Test that prompts use natural, conversational language"""
        prompt = self.prompt_templates.get_agent_prompt()
        system_content = prompt.messages[0].prompt.template.lower()
        
        # Should use natural, friendly language
        assert "help" in system_content
        assert "friendly" in system_content or "conversational" in system_content
        
        # Should not have complex technical jargon or rules
        assert "critical:" not in system_content
        assert "never do these" not in system_content
        assert "follow exactly" not in system_content 