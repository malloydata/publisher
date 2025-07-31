#!/usr/bin/env python3
"""
Test LLM agent chart generation with the unified response processor
Supports both Claude and GPT-4o agents
"""

import os
import sys
import asyncio
import time
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add the project root to the path
project_root = os.path.dirname(__file__)
sys.path.insert(0, project_root)

from src.agents.malloy_langchain_agent import MalloyLangChainAgent

# Test configurations for different models
TEST_CONFIGS = {
    "claude": {
        "model_name": "claude-3.5-sonnet",
        "provider": "anthropic",
        "api_key_env": "ANTHROPIC_API_KEY",
        "test_query": """
        Create a sample bar chart showing sales data by year. Use this sample data:
        - 2020: $1,500,000
        - 2021: $2,200,000  
        - 2022: $2,800,000
        - 2023: $3,100,000
        
        Make it a nice looking chart with proper labels.
        """
    },
    "gpt4o": {
        "model_name": "gpt-4o",
        "provider": "openai", 
        "api_key_env": "OPENAI_API_KEY",
        "test_query": """
        Create a sample pie chart showing market share data. Use this sample data:
        - Company A: 35%
        - Company B: 28%  
        - Company C: 20%
        - Company D: 17%
        
        Make it a colorful pie chart with proper labels and percentages shown.
        """
    }
}

async def test_agent_chart_generation(model_type="claude"):
    """Test chart generation with specified agent type"""
    
    config = TEST_CONFIGS[model_type]
    
    print(f"🔧 Testing Real {config['model_name']} Agent Chart Generation")
    print("=" * 60)
    
    # Check for API key
    api_key = os.environ.get(config["api_key_env"])
    if not api_key:
        print(f"❌ {config['api_key_env']} not set. Please set this environment variable")
        return False
    
    print(f"✅ {config['provider'].title()} API key configured")
    
    # Setup MCP server URL from environment
    mcp_url = os.environ.get("MCP_URL", "http://localhost:3001")  # fallback to old default
    print(f"🔗 Using MCP URL: {mcp_url}")
    
    try:
        # Initialize agent with appropriate configuration
        print(f"\n1️⃣ Initializing {config['model_name']} agent...")
        
        # Prepare agent kwargs based on provider
        agent_kwargs = {
            "mcp_url": mcp_url,
            "model_name": config["model_name"],
            "llm_provider": config["provider"],
            "session_id": f"test_{model_type}_charts"
        }
        
        # Add the appropriate API key
        if config["provider"] == "anthropic":
            agent_kwargs["anthropic_api_key"] = api_key
        elif config["provider"] == "openai":
            agent_kwargs["openai_api_key"] = api_key
        
        agent = MalloyLangChainAgent(**agent_kwargs)
        
        # Setup the agent
        print("2️⃣ Setting up agent...")
        await agent.setup()
        print(f"✅ Agent initialized with {len(agent.tools)} tools")
        
        print(f"\n3️⃣ Testing chart generation...")
        print(f"Query: {config['test_query'][:100]}...")
        
        # Record start time for debugging
        start_time = time.time()
        
        # Process the question
        success, response, metadata = await agent.process_question(config["test_query"])
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n4️⃣ Results (took {duration:.2f}s):")
        print(f"Success: {success}")
        print(f"Response length: {len(response)} chars")
        print(f"Metadata: {metadata}")
        
        # Check if response contains chart info
        if 'chart_url' in response or 'status' in response:
            print("✅ Response contains chart information")
            print(f"Response: {response}")
            
            # Try to parse as JSON to see chart details
            try:
                import json
                chart_data = json.loads(response)
                if 'chart_url' in chart_data:
                    chart_url = chart_data['chart_url']
                    status = chart_data.get('status', 'unknown')
                    if chart_url and status == 'success':
                        print(f"🎨 Chart URL created: {chart_url}")
                        print(f"📊 Chart status: {status}")
                        return True
                    else:
                        print(f"❌ Chart creation failed or no URL: {chart_url}, status: {status}")
                        return False
            except json.JSONDecodeError:
                print("⚠️ Response is not valid JSON, but contains chart keywords")
                print(f"Raw response: {response}")
        else:
            print("❌ Response does not contain chart information")
            print(f"Response: {response}")
            return False
            
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Main test function with argument parsing"""
    parser = argparse.ArgumentParser(description="Test LLM agent chart generation")
    parser.add_argument("--model", choices=["claude", "gpt4o", "both"], default="claude",
                       help="Which model to test (default: claude)")
    
    args = parser.parse_args()
    
    print(f"🚀 Starting LLM Chart Test at {datetime.now()}")
    print("\n🔍 Checking prerequisites...")
    
    results = {}
    
    if args.model == "both":
        # Test both models
        for model_type in ["claude", "gpt4o"]:
            print(f"\n{'='*80}")
            results[model_type] = await test_agent_chart_generation(model_type)
    else:
        # Test single model
        results[args.model] = await test_agent_chart_generation(args.model)
    
    # Summary
    print(f"\n{'='*80}")
    print("📊 FINAL RESULTS:")
    
    all_passed = True
    for model, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {model.upper()}: {status}")
        if not success:
            all_passed = False
    
    if all_passed:
        print("\n🎉 All tests PASSED!")
        print("✅ Unified response processor works correctly for all tested models")
    else:
        print("\n❌ Some tests FAILED!")
        print("❌ Check the logs above for details")
    
    return all_passed

if __name__ == "__main__":
    asyncio.run(main()) 