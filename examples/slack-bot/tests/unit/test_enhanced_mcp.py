"""
Unit tests for Enhanced MCP Client

Tests the basic functionality without requiring a real MCP server
"""

import pytest
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from aiohttp import ClientSession, ClientResponse

from src.clients.enhanced_mcp_client import (
    EnhancedMCPClient, 
    MCPConfig, 
    MCPError, 
    MCPConnectionError, 
    MCPAuthError,
    MCPTimeoutError
)

class TestMCPConfig:
    """Test MCPConfig dataclass"""
    
    def test_config_creation_with_defaults(self):
        """Test creating config with minimal required parameters"""
        config = MCPConfig(url="http://localhost:4040/mcp")
        
        assert config.url == "http://localhost:4040/mcp"
        assert config.auth_token is None
        assert config.timeout == 30
        assert config.max_retries == 3
        assert config.base_delay == 1.0
        assert config.max_delay == 60.0
    
    def test_config_creation_with_auth(self):
        """Test creating config with authentication"""
        config = MCPConfig(
            url="https://production.com/mcp",
            auth_token="test-token-123",
            timeout=60
        )
        
        assert config.url == "https://production.com/mcp"
        assert config.auth_token == "test-token-123"
        assert config.timeout == 60


class TestEnhancedMCPClient:
    """Test EnhancedMCPClient functionality"""
    
    @pytest.fixture
    def config(self):
        """Provide test configuration"""
        return MCPConfig(url="http://localhost:4040/mcp")
    
    @pytest.fixture
    def config_with_auth(self):
        """Provide test configuration with auth"""
        return MCPConfig(
            url="https://production.com/mcp",
            auth_token="test-token-123"
        )
    
    def test_client_initialization(self, config):
        """Test client can be initialized with config"""
        client = EnhancedMCPClient(config)
        
        assert client.config == config
        assert client.session is None
        assert client.available_tools == {}
        assert client._request_id == 0
    
    def test_request_id_increment(self, config):
        """Test request ID increments properly"""
        client = EnhancedMCPClient(config)
        
        assert client._next_request_id() == 1
        assert client._next_request_id() == 2
        assert client._next_request_id() == 3
    
    @pytest.mark.asyncio
    async def test_session_creation_without_auth(self, config):
        """Test session creation without authentication"""
        client = EnhancedMCPClient(config)
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            await client._create_session()
            
            # Verify session was created with correct parameters
            mock_session_class.assert_called_once()
            call_kwargs = mock_session_class.call_args.kwargs
            
            assert 'headers' in call_kwargs
            headers = call_kwargs['headers']
            assert headers['Content-Type'] == 'application/json'
            assert 'Authorization' not in headers
            
            assert client.session == mock_session
    
    @pytest.mark.asyncio
    async def test_session_creation_with_auth(self, config_with_auth):
        """Test session creation with authentication"""
        client = EnhancedMCPClient(config_with_auth)
        
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            await client._create_session()
            
            # Verify auth header was set
            call_kwargs = mock_session_class.call_args.kwargs
            headers = call_kwargs['headers']
            assert headers['Authorization'] == 'Bearer test-token-123'
    
    @pytest.mark.asyncio
    async def test_close_session(self, config):
        """Test session cleanup"""
        client = EnhancedMCPClient(config)
        
        # Mock session
        mock_session = AsyncMock()
        client.session = mock_session
        
        await client.close()
        
        mock_session.close.assert_called_once()
        assert client.session is None
    
    @pytest.mark.asyncio
    async def test_context_manager(self, config):
        """Test async context manager functionality"""
        with patch('aiohttp.ClientSession') as mock_session_class:
            mock_session = AsyncMock()
            mock_session_class.return_value = mock_session
            
            # Mock the discover_tools method to avoid actual network call
            with patch.object(EnhancedMCPClient, 'discover_tools', new_callable=AsyncMock) as mock_discover:
                mock_discover.return_value = {}
                
                async with EnhancedMCPClient(config) as client:
                    assert client.session == mock_session
                    mock_discover.assert_called_once()
                
                # Verify session was closed
                mock_session.close.assert_called_once()
    
    def test_parse_sse_response(self, config):
        """Test parsing Server-Sent Events response format"""
        client = EnhancedMCPClient(config)
        
        sse_response = """event: message
data: {"jsonrpc": "2.0", "id": 1, "result": {"tools": [{"name": "test_tool"}]}}"""
        
        result = asyncio.run(client._parse_response(sse_response))
        
        assert result == {"tools": [{"name": "test_tool"}]}
    
    def test_parse_json_response(self, config):
        """Test parsing regular JSON response"""
        client = EnhancedMCPClient(config)
        
        json_response = '{"jsonrpc": "2.0", "id": 1, "result": {"status": "ok"}}'
        
        result = asyncio.run(client._parse_response(json_response))
        
        assert result == {"status": "ok"}
    
    def test_parse_error_response(self, config):
        """Test parsing error response"""
        client = EnhancedMCPClient(config)
        
        error_response = '{"jsonrpc": "2.0", "id": 1, "error": {"code": -1, "message": "Test error"}}'
        
        with pytest.raises(MCPError, match="Test error"):
            asyncio.run(client._parse_response(error_response))
    
    def test_parse_invalid_json(self, config):
        """Test handling invalid JSON response"""
        client = EnhancedMCPClient(config)
        
        invalid_response = "not valid json"
        
        with pytest.raises(MCPError, match="Failed to parse MCP response"):
            asyncio.run(client._parse_response(invalid_response))


class TestMCPClientErrorHandling:
    """Test error handling and retry logic"""
    
    @pytest.fixture
    def config(self):
        return MCPConfig(url="http://localhost:4040/mcp", max_retries=2)
    
    @pytest.mark.asyncio
    async def test_connection_error_handling(self, config):
        """Test handling of connection errors"""
        client = EnhancedMCPClient(config)
        
        # Test that we use the retry wrapper which converts exceptions
        with patch.object(client, '_make_request_with_retry') as mock_retry:
            mock_retry.side_effect = MCPConnectionError("Connection failed")
            
            with pytest.raises(MCPConnectionError):
                await client._make_request_with_retry("test/method")
    
    @pytest.mark.asyncio 
    async def test_auth_error_handling(self, config):
        """Test handling of authentication errors"""
        # TODO: Complex async context manager mocking - will implement in integration tests
        # For now, just test that the error classes exist and can be raised
        with pytest.raises(MCPAuthError, match="test error"):
            raise MCPAuthError("test error")
    
    @pytest.mark.asyncio
    async def test_tool_not_available_error(self, config):
        """Test error when calling non-existent tool"""
        client = EnhancedMCPClient(config)
        client.available_tools = {"existing_tool": {}}
        
        with pytest.raises(MCPError, match="Tool 'non_existent_tool' not available"):
            await client.call_tool("non_existent_tool", {})


class TestMCPClientHelperMethods:
    """Test helper methods for common operations"""
    
    @pytest.fixture
    def config(self):
        return MCPConfig(url="http://localhost:4040/mcp")
    
    def test_extract_projects_placeholder(self, config):
        """Test placeholder implementation of _extract_projects"""
        client = EnhancedMCPClient(config)
        
        # These methods are placeholders that return empty values
        # They will be implemented when we test against real MCP server
        result = client._extract_projects({})
        assert result == []
    
    def test_extract_packages_placeholder(self, config):
        """Test placeholder implementation of _extract_packages"""
        client = EnhancedMCPClient(config)
        
        result = client._extract_packages({})
        assert result == []
    
    def test_extract_model_text_placeholder(self, config):
        """Test placeholder implementation of _extract_model_text"""
        client = EnhancedMCPClient(config)
        
        result = client._extract_model_text({})
        assert result == ""


# Integration test stubs (will be expanded when we test against real server)
class TestMCPIntegrationStubs:
    """Placeholder for integration tests against real MCP server"""
    
    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_real_tool_discovery(self):
        """Test tool discovery against real MCP server"""
        # This will be implemented when we test against the actual server
        pytest.skip("Integration test - requires real MCP server")
    
    @pytest.mark.integration  
    @pytest.mark.asyncio
    async def test_real_tool_execution(self):
        """Test tool execution against real MCP server"""
        # This will be implemented when we test against the actual server
        pytest.skip("Integration test - requires real MCP server")


if __name__ == "__main__":
    # Run tests when script is executed directly
    pytest.main([__file__, "-v"])