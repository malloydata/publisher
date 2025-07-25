"""
Matplotlib Chart Generation Tool
Creates charts using matplotlib with sandboxed Python execution
"""
import json
import uuid
import os
import tempfile
from typing import Dict, Any, Optional
from langchain.tools import BaseTool
from pydantic import BaseModel, Field
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

matplotlib.use('Agg')  # Use non-interactive backend


class MatplotlibChartInput(BaseModel):
    """Input for Matplotlib chart tool"""
    python_code: str = Field(
        description="Python code that creates and saves a matplotlib chart. MUST use the provided 'filepath' variable for saving: plt.savefig(filepath). Available variables: 'filepath', 'chart_filename'. DO NOT use hardcoded paths like '/mnt/data/'."
    )


class MatplotlibChartTool(BaseTool):
    """Tool for generating charts using matplotlib with sandboxed execution"""

    name: str = "generate_chart"
    description: str = (
        "Generates data visualizations by executing Python code with matplotlib. "
        "When chart generation fails, returns detailed error information to help with debugging. "
        "Use this tool after getting data from queries to create charts and graphs. "
        
        "Guidelines for successful charts:"
        "- Parse the data from previous tool results"
        "- Use matplotlib.pyplot for visualization" 
        "- IMPORTANT: Use the provided 'filepath' variable for saving: plt.savefig(filepath)"
        "- DO NOT use hardcoded paths like '/mnt/data/' - use the provided 'filepath' variable"
        "- Handle different data formats gracefully"
        "- Choose appropriate chart types (bar, line, pie, etc.)"
        "- Add clear titles and axis labels"
        "- Always call plt.close() after saving to free memory"
    )
    args_schema: type[BaseModel] = MatplotlibChartInput

    def _run(self, python_code: str) -> str:
        """Execute matplotlib code to generate chart with better error handling"""
        try:
            # Use current directory for charts to avoid permission issues
            chart_filename = f"chart_{uuid.uuid4().hex[:8]}.png"
            filepath = os.path.abspath(chart_filename)
            
            # Get list of PNG files before execution to detect new ones
            import glob
            existing_pngs = set(glob.glob("*.png"))
            
            safe_globals = {
                "pd": pd,
                "plt": plt,
                "json": json,
                "filepath": filepath,
                "chart_filename": chart_filename,
                "os": os
            }
            
            # Pre-process the code to fix common path issues
            processed_code = python_code.replace("/mnt/data/", "./")
            processed_code = processed_code.replace("'/mnt/data'", "'.'")
            processed_code = processed_code.replace('"/mnt/data"', '"."')
            
            # More comprehensive replacements for full paths
            import re
            # Replace any /mnt/data/filename.png with just filename.png
            processed_code = re.sub(r'/mnt/data/([^\'\"]*\.png)', r'\1', processed_code)
            # Replace any plt.savefig('/mnt/data/...') with plt.savefig(filepath)
            processed_code = re.sub(r'plt\.savefig\([\'\"]/mnt/data/[^\'\"]*[\'\"]\)', 'plt.savefig(filepath)', processed_code)
            
            print(f"🔍 DEBUG: Original code: {python_code[:100]}...")
            print(f"🔍 DEBUG: Processed code: {processed_code[:100]}...")
            
            local_vars = {}
            exec(processed_code, safe_globals, local_vars)
            
            # Check if file was saved to expected path
            if os.path.exists(filepath):
                return json.dumps({
                    "text": "Chart created successfully!",
                    "file_info": {"status": "success", "filepath": filepath}
                })
            
            # Check for new PNG files in current directory
            current_pngs = set(glob.glob("*.png"))
            new_pngs = current_pngs - existing_pngs
            
            if new_pngs:
                # Found a new chart file in current directory
                chart_file = list(new_pngs)[0]  # Take the first new PNG
                full_path = os.path.abspath(chart_file)
                return json.dumps({
                    "text": "Chart created successfully!",
                    "file_info": {"status": "success", "filepath": full_path}
                })
            else:
                return json.dumps({
                    "text": "Chart generation failed - no file was created",
                    "file_info": {
                        "status": "error",
                        "error_type": "no_file_created",
                        "message": "The code executed but no chart file was saved. Make sure to call plt.savefig(filepath) or plt.savefig('filename.png').",
                        "suggestion": "Add plt.savefig(filepath) at the end of your plotting code"
                    }
                })
                
        except SyntaxError as e:
            return json.dumps({
                "text": "Chart generation failed due to syntax error",
                "file_info": {
                    "status": "error",
                    "error_type": "syntax_error", 
                    "message": f"Python syntax error: {str(e)}",
                    "line_number": getattr(e, 'lineno', None),
                    "suggestion": "Check your Python code for syntax errors"
                }
            })
        except Exception as e:
            plt.close('all')
            error_type = type(e).__name__
            return json.dumps({
                "text": f"Chart generation failed: {error_type}",
                "file_info": {
                    "status": "error",
                    "error_type": error_type.lower(),
                    "message": str(e),
                    "suggestion": "Check your data format and plotting code. Make sure the data exists and is in the expected format."
                }
            })
        
        finally:
            plt.close('all')

    async def _arun(self, python_code: str) -> str:
        """Async version of chart generation"""
        return self._run(python_code)


def create_matplotlib_chart_tool() -> MatplotlibChartTool:
    """Create a matplotlib chart tool instance"""
    return MatplotlibChartTool()


# Example usage function
def test_matplotlib_chart_tool():
    """Test the matplotlib chart tool with sample data"""
    
    tool = create_matplotlib_chart_tool()
    
    # Sample Python code for chart generation
    sample_code = """
import pandas as pd
import matplotlib.pyplot as plt
import json

# Sample data
data = {
    "labels": ["Levi's", "Ray-Ban", "Columbia", "Carhartt", "Dockers"],
    "values": [1772930.94, 867047.23, 780730.58, 549446.06, 494120.49]
}

# Create DataFrame
df = pd.DataFrame(data)

# Create the chart
plt.figure(figsize=(10, 6))
plt.bar(df['labels'], df['values'], color=['#42A5F5', '#66BB6A', '#FFA726', '#EF5350', '#AB47BC'])
plt.title('Total Sales by Brand')
plt.xlabel('Product Brand')
plt.ylabel('Total Sales')
plt.xticks(rotation=45)
plt.tight_layout()

# Save the chart (REQUIRED!)
plt.savefig(filepath, dpi=300, bbox_inches='tight')
plt.close()
"""
    
    # Test the tool
    result = tool._run(sample_code)
    print("Matplotlib chart tool result:")
    print(result)
    
    return result


if __name__ == "__main__":
    test_matplotlib_chart_tool()