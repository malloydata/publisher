#!/usr/bin/env python3
"""
Test the matplotlib chart tool directly and unified chart response processing
"""

import json
import os
import sys
import tempfile
from unittest.mock import Mock, patch

# Add the project root to the path so we can import modules
project_root = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, project_root)

from src.tools.matplotlib_chart_tool import MatplotlibChartTool
from src.agents.malloy_langchain_agent import MalloyLangChainAgent

def test_unified_chart_response_processor():
    """Test that the unified chart response processor works for both OpenAI and Claude agents"""
    
    print("🔧 Testing Unified Chart Response Processor")
    print("=" * 50)
    
    # Create a mock agent (we don't need full initialization for this test)
    agent = Mock()
    agent._extract_chart_json_response = MalloyLangChainAgent._extract_chart_json_response.__get__(agent)
    agent._construct_chart_fallback = MalloyLangChainAgent._construct_chart_fallback.__get__(agent)
    
    # Test Case 1: OpenAI agent with proper intermediate_steps
    print("\n1. Testing OpenAI agent with proper intermediate_steps")
    
    # Mock an intermediate step that contains chart tool execution
    mock_action = Mock()
    mock_action.tool = "generate_chart"
    chart_result = json.dumps({
        "text": "Chart created successfully!",
        "file_info": {"status": "success", "filepath": "/tmp/test_chart.png"}
    })
    
    openai_result = {
        "output": "I've created a chart showing the sales data.",
        "intermediate_steps": [(mock_action, chart_result)]
    }
    
    # Test extraction
    extracted = agent._extract_chart_json_response(openai_result)
    assert extracted == chart_result, f"Expected {chart_result}, got {extracted}"
    print("✅ OpenAI agent intermediate_steps extraction works")
    
    # Test Case 2: Claude agent with proper intermediate_steps (should work the same way now)
    print("\n2. Testing Claude agent with proper intermediate_steps")
    
    claude_result = {
        "output": ["I've created a chart showing the sales data."],  # Claude returns list format
        "intermediate_steps": [(mock_action, chart_result)]
    }
    
    extracted = agent._extract_chart_json_response(claude_result)
    assert extracted == chart_result, f"Expected {chart_result}, got {extracted}"
    print("✅ Claude agent intermediate_steps extraction works")
    
    # Test Case 3: No intermediate_steps (fallback scenarios)
    print("\n3. Testing fallback when no intermediate_steps available")
    
    no_steps_result = {
        "output": "I created a chart but intermediate_steps are missing",
        "intermediate_steps": []
    }
    
    extracted = agent._extract_chart_json_response(no_steps_result)
    assert extracted is None, f"Expected None, got {extracted}"
    print("✅ Fallback handling works when no intermediate_steps")
    
    # Test Case 4: Malformed intermediate_steps
    print("\n4. Testing with malformed intermediate_steps")
    
    malformed_result = {
        "output": "Chart created",
        "intermediate_steps": [("invalid", "format")]
    }
    
    extracted = agent._extract_chart_json_response(malformed_result)
    assert extracted is None, f"Expected None, got {extracted}"
    print("✅ Malformed intermediate_steps handled gracefully")
    
    print("\n🎉 All unified chart response processor tests passed!")

def test_matplotlib_tool():
    """Test the matplotlib tool directly"""
    
    print("🎨 Testing Matplotlib Chart Tool")
    print("=" * 40)
    
    # Create tool
    tool = MatplotlibChartTool()
    
    # Test with sample data similar to what the agent would generate
    sample_code = '''
import pandas as pd
import matplotlib.pyplot as plt
import json

# Sample data that mimics Malloy query results
data = {
    "years": [2019, 2020, 2021, 2022],
    "sales": [1318595.54, 2602043.07, 3900910.81, 4744743.45]
}

# Create DataFrame
df = pd.DataFrame(data)

# Create chart
plt.figure(figsize=(10, 6))
plt.bar(df['years'], df['sales'], color='skyblue')
plt.title('Total Sales by Year')
plt.xlabel('Year')
plt.ylabel('Total Sales ($)')
plt.xticks(df['years'])
plt.grid(axis='y', alpha=0.3)

# Format y-axis to show currency
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

plt.tight_layout()

# Save chart (REQUIRED!)
plt.savefig(filepath, dpi=300, bbox_inches='tight')
plt.close()
'''
    
    print("Running matplotlib code...")
    try:
        result = tool._run(sample_code)
        print(f"✅ Tool executed successfully")
        print(f"Result: {result}")
        
        # Parse result
        try:
            parsed = json.loads(result)
            file_info = parsed.get('file_info', {})
            
            if file_info.get('status') == 'success':
                file_path = file_info.get('filepath')
                print(f"✅ Chart created: {file_path}")
                
                if os.path.exists(file_path):
                    print(f"✅ File exists and is {os.path.getsize(file_path)} bytes")
                else:
                    print(f"❌ File doesn't exist at {file_path}")
            else:
                print(f"❌ Chart creation failed: {file_info.get('message', 'Unknown error')}")
                
        except json.JSONDecodeError:
            print(f"❌ Tool result is not JSON: {result}")
            
    except Exception as e:
        print(f"❌ Tool execution failed: {e}")
        import traceback
        traceback.print_exc()

def test_complex_json_parsing():
    """Test parsing complex JSON data like from Malloy"""
    
    print("\n🧪 Testing Complex JSON Parsing")
    print("=" * 40)
    
    tool = MatplotlibChartTool()
    
    # Complex code that handles nested Malloy data structure
    complex_code = '''
import pandas as pd
import matplotlib.pyplot as plt
import json

# Complex nested data structure like from Malloy
result = {
    "data": {
        "array_value": [
            {"record_value": [{"number_value": 2022}, {"number_value": 4744743.45}]},
            {"record_value": [{"number_value": 2021}, {"number_value": 3900910.81}]},
            {"record_value": [{"number_value": 2020}, {"number_value": 2602043.07}]},
            {"record_value": [{"number_value": 2019}, {"number_value": 1318595.54}]}
        ]
    }
}

# Extract data from nested structure
years = []
sales = []

for row in result["data"]["array_value"]:
    year = row["record_value"][0]["number_value"]
    sale = row["record_value"][1]["number_value"]
    years.append(year)
    sales.append(sale)

# Create DataFrame
df = pd.DataFrame({"Year": years, "Sales": sales})

# Create chart
plt.figure(figsize=(10, 6))
plt.bar(df['Year'], df['Sales'], color=['#42A5F5', '#66BB6A', '#FFA726', '#EF5350'])
plt.title('Total Sales by Year')
plt.xlabel('Year')
plt.ylabel('Total Sales ($)')
plt.xticks(df['Year'])
plt.grid(axis='y', alpha=0.3)

# Format y-axis
plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x:,.0f}'))

plt.tight_layout()
plt.savefig(filepath, dpi=300, bbox_inches='tight')
plt.close()
'''
    
    print("Running complex JSON parsing code...")
    try:
        result = tool._run(complex_code)
        print(f"✅ Complex parsing successful")
        
        parsed = json.loads(result)
        file_info = parsed.get('file_info', {})
        
        if file_info.get('status') == 'success':
            file_path = file_info.get('filepath')
            print(f"✅ Complex chart created: {file_path}")
            
            if os.path.exists(file_path):
                print(f"✅ File exists and is {os.path.getsize(file_path)} bytes")
        else:
            print(f"❌ Complex chart failed: {file_info.get('message')}")
            
    except Exception as e:
        print(f"❌ Complex parsing failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_unified_chart_response_processor()
    test_matplotlib_tool()
    test_complex_json_parsing()