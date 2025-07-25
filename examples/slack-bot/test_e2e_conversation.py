#!/usr/bin/env python3
"""
End-to-end test simulating a real Slack conversation flow:
1. "show me the top 5 names for boys"
2. "and what about for girls" 
3. "graph the girls names"

This tests the complete pipeline: Malloy queries → memory/context → chart generation
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

async def test_e2e_conversation(model_type="claude"):
    """Test end-to-end conversation flow"""
    
    print(f"🎭 E2E Conversation Test with {model_type.upper()}")
    print("=" * 60)
    
    # Configuration for different models
    configs = {
        "claude": {
            "model_name": "claude-3.5-sonnet",
            "provider": "anthropic",
            "api_key_env": "ANTHROPIC_API_KEY"
        },
        "gpt4o": {
            "model_name": "gpt-4o",
            "provider": "openai", 
            "api_key_env": "OPENAI_API_KEY"
        }
    }
    
    config = configs[model_type]
    
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
        # Initialize agent
        print(f"\n1️⃣ Initializing {config['model_name']} agent...")
        
        agent_kwargs = {
            "mcp_url": mcp_url,
            "model_name": config["model_name"],
            "llm_provider": config["provider"],
            "session_id": f"e2e_test_{model_type}_{int(time.time())}"
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
        
        # Define the conversation flow
        conversation_turns = [
            {
                "turn": 1,
                "query": "show me the top 5 names for boys",
                "expected": ["query", "data", "boys", "names"],
                "description": "Initial query for boys names data"
            },
            {
                "turn": 2, 
                "query": "and what about for girls",
                "expected": ["girls", "names", "data"],
                "description": "Follow-up query using context"
            },
            {
                "turn": 3,
                "query": "graph the girls names",
                "expected": ["chart", "file_info", "filepath"],
                "description": "Chart generation from previous data"
            }
        ]
        
        print(f"\n3️⃣ Starting conversation simulation...")
        results = []
        
        for turn_info in conversation_turns:
            turn_num = turn_info["turn"]
            query = turn_info["query"]
            expected_keywords = turn_info["expected"]
            description = turn_info["description"]
            
            print(f"\n{'='*50}")
            print(f"💬 TURN {turn_num}: {description}")
            print(f"Query: \"{query}\"")
            print(f"Expected keywords: {expected_keywords}")
            
            start_time = time.time()
            
            # Process the question
            success, response, metadata = await agent.process_question(query)
            
            duration = time.time() - start_time
            
            print(f"\n📊 Results (took {duration:.2f}s):")
            print(f"Success: {success}")
            print(f"Response length: {len(response)} chars")
            
            # Check for expected keywords
            response_lower = response.lower()
            found_keywords = [kw for kw in expected_keywords if kw in response_lower]
            missing_keywords = [kw for kw in expected_keywords if kw not in response_lower]
            
            print(f"Found keywords: {found_keywords}")
            if missing_keywords:
                print(f"Missing keywords: {missing_keywords}")
            
            # Special handling for chart turn
            if turn_num == 3:
                # Check if this is a chart response
                try:
                    import json
                    chart_data = json.loads(response)
                    if 'file_info' in chart_data:
                        file_path = chart_data['file_info'].get('filepath')
                        if file_path and os.path.exists(file_path):
                            file_size = os.path.getsize(file_path)
                            print(f"🎨 Chart created: {file_path}")
                            print(f"📊 File size: {file_size} bytes")
                        else:
                            print(f"❌ Chart file not found: {file_path}")
                except json.JSONDecodeError:
                    if any(kw in response_lower for kw in ["chart", "png", "graph"]):
                        print("⚠️ Response mentions charts but not in expected JSON format")
            
            # Show first 200 chars of response
            preview = response[:200] + "..." if len(response) > 200 else response
            print(f"Response preview: {preview}")
            
            turn_result = {
                "turn": turn_num,
                "success": success,
                "duration": duration,
                "found_keywords": found_keywords,
                "missing_keywords": missing_keywords,
                "response_length": len(response)
            }
            
            results.append(turn_result)
            
            # Brief pause between turns
            await asyncio.sleep(1)
        
        # Summary
        print(f"\n{'='*60}")
        print("📋 CONVERSATION SUMMARY:")
        
        all_successful = True
        for result in results:
            turn_status = "✅ SUCCESS" if result["success"] else "❌ FAILED"
            keyword_status = f"({len(result['found_keywords'])}/{len(result['found_keywords']) + len(result['missing_keywords'])} keywords)"
            print(f"  Turn {result['turn']}: {turn_status} {keyword_status} ({result['duration']:.1f}s)")
            if not result["success"] or result["missing_keywords"]:
                all_successful = False
        
        return all_successful
            
    except Exception as e:
        print(f"❌ Error during E2E test: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Main test function with argument parsing"""
    parser = argparse.ArgumentParser(description="Run E2E conversation test")
    parser.add_argument("--model", choices=["claude", "gpt4o"], default="claude",
                       help="Which model to test (default: claude)")
    
    args = parser.parse_args()
    
    print(f"🚀 Starting E2E Conversation Test at {datetime.now()}")
    print(f"🎯 Testing multi-turn conversation flow with {args.model.upper()}")
    print("\n🔍 Checking prerequisites...")
    
    # Test the conversation flow
    success = await test_e2e_conversation(args.model)
    
    if success:
        print(f"\n🎉 E2E conversation test PASSED!")
        print("✅ Multi-turn conversation with data queries and chart generation works correctly")
    else:
        print(f"\n❌ E2E conversation test FAILED!")
        print("❌ Check the logs above for details")
    
    return success

if __name__ == "__main__":
    asyncio.run(main()) 