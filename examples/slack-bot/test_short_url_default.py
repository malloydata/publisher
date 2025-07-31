#!/usr/bin/env python3
"""
Quick test for new short URL default behavior
"""
import sys
import os
import json

# Add the project root to the path
sys.path.insert(0, os.path.dirname(__file__))

from src.tools.quickchart_tool import QuickChartTool

def main():
    print("🧪 Testing new short URL default behavior...")
    
    tool = QuickChartTool()
    
    sample_config = {
        "type": "line",
        "data": {
            "labels": ["Jan", "Feb", "Mar"],
            "datasets": [{
                "label": "Growth",
                "data": [10, 25, 40],
                "borderColor": "#36a2eb"
            }]
        }
    }
    
    # Test with default parameters (should now use short URL)
    result = tool._run(sample_config, width=500, height=300, title="Default Test")
    
    try:
        result_data = json.loads(result)
        url = result_data['chart_url']
        url_type = result_data.get('url_type', 'unknown')
        
        print(f"✅ Tool executed successfully!")
        print(f"   URL Type: {url_type}")
        print(f"   URL: {url}")
        
        if url_type == "short" and "quickchart.io/chart/render/" in url:
            print(f"✅ Short URL is now the default - perfect for Slack!")
            return True
        else:
            print(f"❌ Expected short URL by default")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    print(f"\nResult: {'✅ PASS' if success else '❌ FAIL'}") 