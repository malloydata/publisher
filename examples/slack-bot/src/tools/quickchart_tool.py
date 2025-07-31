"""
QuickChart.io Chart Generation Tool
Creates charts using Chart.js configurations via QuickChart.io web service
"""
import json
import requests
from typing import Dict, Any
from langchain.tools import BaseTool
from pydantic import BaseModel, Field

try:
    from quickchart import QuickChart
except ImportError:
    QuickChart = None


class QuickChartInput(BaseModel):
    """Input for QuickChart tool"""
    chart_config: dict = Field(
        description=(
            "Chart.js configuration object. Must include 'type' (bar, line, pie, doughnut, scatter, radar, etc.) "
            "and 'data' with labels and datasets. "
            "\n\nExample: "
            "{\n"
            "  'type': 'bar',\n"
            "  'data': {\n"
            "    'labels': ['January', 'February', 'March'],\n"
            "    'datasets': [{\n"
            "      'label': 'Sales',\n"
            "      'data': [100, 150, 200],\n"
            "      'backgroundColor': ['#ff6384', '#36a2eb', '#cc65fe']\n"
            "    }]\n"
            "  },\n"
            "  'options': {\n"
            "    'responsive': true,\n"
            "    'plugins': {\n"
            "      'title': {\n"
            "        'display': true,\n"
            "        'text': 'Monthly Sales'\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}"
        )
    )
    width: int = Field(default=500, description="Chart width in pixels")
    height: int = Field(default=300, description="Chart height in pixels")
    title: str = Field(default="", description="Optional chart title to add to the chart")
    use_short_url: bool = Field(
        default=True, 
        description=(
            "Generate a short URL for the chart (e.g., https://quickchart.io/chart/render/f-a1d3e804-...). "
            "Short URLs are clean and ideal for sharing in Slack/chat. Set to False only if you need "
            "a long-term stable URL for embedding (short URLs expire after a few days for free users)."
        )
    )


class QuickChartTool(BaseTool):
    """Tool for generating charts using QuickChart.io service"""

    name: str = "generate_chart"
    description: str = (
        "Generate data visualizations using Chart.js configurations via QuickChart.io. "
        "Returns a URL to the chart image that can be shared or embedded. "
        "This tool is perfect for creating professional charts from data.\n\n"
        
        "Supported chart types: bar, line, pie, doughnut, scatter, radar, polarArea, bubble, and more.\n\n"
        
        "Chart.js configuration guidelines:\n"
        "- REQUIRED: 'type' field (e.g., 'bar', 'line', 'pie')\n"
        "- REQUIRED: 'data' object with 'labels' array and 'datasets' array\n"
        "- Each dataset needs 'label' and 'data' fields\n"
        "- Optional: 'options' for customization (titles, legends, axes, etc.)\n"
        "- Optional: 'backgroundColor' and 'borderColor' for styling\n\n"
        
        "URL Options (defaults to short URLs for Slack sharing):\n"
        "- Short URLs (DEFAULT): Clean, shareable URLs ideal for Slack/chat conversations\n"
        "  * Format: https://quickchart.io/chart/render/f-a1d3e804-...\n"
        "  * Perfect for immediate sharing, expire after a few days for free users\n"
        "- Regular URLs: Set use_short_url=False for long-term stable URLs for embedding\n\n"
        
        "Example usage workflow:\n"
        "1. Get data from previous query results\n"
        "2. Format data into Chart.js structure\n"
        "3. Choose appropriate chart type\n"
        "4. Add clear labels and titles\n"
        "5. Call this tool with the configuration"
    )
    args_schema: type[BaseModel] = QuickChartInput

    def _generate_short_url(self, chart_config: dict, width: int, height: int) -> str:
        """Generate a short URL using QuickChart.io API"""
        try:
            payload = {
                "chart": chart_config,
                "width": width,
                "height": height
            }
            
            response = requests.post(
                "https://quickchart.io/chart/create",
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            if "url" in result:
                return result["url"]
            else:
                raise ValueError("No URL returned from QuickChart.io API")
                
        except requests.RequestException as e:
            raise Exception(f"Failed to create short URL: {str(e)}")
        except Exception as e:
            raise Exception(f"Short URL generation error: {str(e)}")

    def _run(self, chart_config: dict, width: int = 500, height: int = 300, title: str = "", use_short_url: bool = True) -> str:
        """Generate chart using QuickChart.io and return URL"""
        
        if QuickChart is None:
            return json.dumps({
                "text": "Chart generation failed: quickchart.io library not installed",
                "status": "error",
                "error": "missing_dependency",
                "suggestion": "Install the quickchart.io library: pip install quickchart.io"
            })
        
        try:
            # Validate basic chart config structure
            if not isinstance(chart_config, dict):
                raise ValueError("chart_config must be a dictionary")
            
            if "type" not in chart_config:
                raise ValueError("chart_config must include a 'type' field (e.g., 'bar', 'line', 'pie')")
            
            if "data" not in chart_config:
                raise ValueError("chart_config must include a 'data' field with labels and datasets")
            
            # Make a copy to avoid modifying the original
            config = chart_config.copy()
            
            # Add title to config if provided and not already present
            if title:
                if "options" not in config:
                    config["options"] = {}
                if "plugins" not in config["options"]:
                    config["options"]["plugins"] = {}
                if "title" not in config["options"]["plugins"]:
                    config["options"]["plugins"]["title"] = {
                        "display": True,
                        "text": title
                    }
            
            # Generate chart URL
            if use_short_url:
                # Use short URL generation via API
                chart_url = self._generate_short_url(config, width, height)
                url_type = "short"
                url_note = "Short URL - expires after a few days for free users"
            else:
                # Use regular QuickChart URL generation
                if QuickChart is None:
                    raise Exception("quickchart.io library required for regular URLs")
                    
                qc = QuickChart()
                qc.width = width
                qc.height = height
                qc.config = config
                chart_url = qc.get_url()
                url_type = "regular"
                url_note = "Regular URL - stable for long-term use"
            
            return json.dumps({
                "text": f"Chart created successfully! ({url_type} URL)",
                "chart_url": chart_url,
                "status": "success",
                "width": width,
                "height": height,
                "chart_type": config.get("type", "unknown"),
                "url_type": url_type,
                "url_note": url_note
            })
            
        except ValueError as e:
            return json.dumps({
                "text": f"Chart configuration error: {str(e)}",
                "status": "error",
                "error": "invalid_config",
                "suggestion": "Check your Chart.js configuration. Ensure it has 'type' and 'data' fields."
            })
        except Exception as e:
            error_msg = str(e)
            return json.dumps({
                "text": f"Chart generation failed: {error_msg}",
                "status": "error",
                "error": "quickchart_api_error",
                "suggestion": "Check your internet connection and try again. If the error persists, try a simpler chart configuration."
            })

    async def _arun(self, chart_config: dict, width: int = 500, height: int = 300, title: str = "", use_short_url: bool = True) -> str:
        """Async version of chart generation"""
        return self._run(chart_config, width, height, title, use_short_url)


def create_quickchart_tool() -> QuickChartTool:
    """Create a QuickChart tool instance"""
    return QuickChartTool()

