#!/usr/bin/env python3
import sys
import json
import urllib.request
import urllib.parse
import urllib.error
import logging
import time
import signal
from typing import Dict, Any, Optional
import os

# Define log file path
log_file = '/tmp/malloy_bridge.log'

# Create directory if it doesn't exist
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Set up logging with more detailed format
logging.basicConfig(
    filename=log_file, 
    level=logging.DEBUG, 
    format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
)

class ImprovedMalloyMCPBridge:
    def __init__(self):
        self.mcp_url = "http://localhost:4040/mcp"
        # No tool name mapping needed - server already uses underscore format

        # Set stdin/stdout to line buffered mode for better responsiveness
        try:
            sys.stdin.reconfigure(line_buffering=True)
            sys.stdout.reconfigure(line_buffering=True)
        except AttributeError:
            # reconfigure not available in older Python versions
            pass

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        logging.info("Bridge initialized with improved stdio handling")

    def signal_handler(self, signum, frame):
        logging.info(f"Received signal {signum}, shutting down gracefully")
        sys.exit(0)

    def parse_sse_response(self, response, request_id: Optional[Any] = None) -> Dict[str, Any]:
        """Parse Server-Sent Events response format with improved error handling"""
        logging.debug(f"Starting SSE parsing for request {request_id}")
        
        try:
            # Read response with size limit to prevent memory issues
            max_size = 1024 * 1024  # 1MB limit
            response_text = ""
            bytes_read = 0
            
            # Read response in chunks to avoid memory issues
            while True:
                chunk = response.read(8192)  # 8KB chunks
                if not chunk:
                    break
                bytes_read += len(chunk)
                if bytes_read > max_size:
                    logging.warning(f"Response too large ({bytes_read} bytes), truncating")
                    break
                response_text += chunk.decode('utf-8')
            
            logging.debug(f"Read {bytes_read} bytes from SSE response")
            
            lines = response_text.strip().split('\n')
            data_line = None

            # Look for data line in SSE format
            for line in lines:
                if line.startswith('data: '):
                    data_line = line[6:]  # Remove 'data: ' prefix
                    break

            # If no SSE format, try parsing the entire response as JSON
            if not data_line:
                data_line = response_text.strip()

            if data_line:
                try:
                    parsed_data = json.loads(data_line)
                    # Ensure the response has a proper ID
                    if 'id' not in parsed_data or parsed_data['id'] is None:
                        parsed_data['id'] = request_id

                    logging.debug(f"Successfully parsed response for request {request_id}")
                    return parsed_data

                except json.JSONDecodeError as e:
                    logging.error(f"JSON decode error for request {request_id}: {e}")
                    logging.error(f"Raw data that failed to parse: {data_line[:200]}...")
                    return {
                        "jsonrpc": "2.0",
                        "error": {
                            "code": -32603,
                            "message": f"Failed to parse response: {str(e)}"
                        },
                        "id": request_id
                    }
            else:
                logging.error(f"No data found in response for request {request_id}")
                return {
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32603,
                        "message": "No data found in response"
                    },
                    "id": request_id
                }
                
        except Exception as e:
            logging.error(f"Error reading SSE response for request {request_id}: {e}")
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": f"SSE parsing error: {str(e)}"
                },
                "id": request_id
            }

    def send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a JSON-RPC request to the Malloy MCP endpoint with improved timeout handling"""
        request_id = request.get("id")
        method = request.get("method", "unknown")
        start_time = time.time()

        logging.debug(f"Sending request {request_id} ({method}) to Malloy server")
        
        # Log ALL method calls for debugging
        logging.info(f"METHOD CALL - Request {request_id}: method='{method}'")
        if method == "tools/call":
            tool_name = request.get("params", {}).get("name", "unknown")
            logging.info(f"TOOL CALL - Request {request_id}: tool_name='{tool_name}'")
        logging.info(f"FULL REQUEST - {json.dumps(request)}")

        try:
            # Ensure the request has proper structure
            if "jsonrpc" not in request:
                request["jsonrpc"] = "2.0"

            # Prepare the request with longer timeout for tools/list
            timeout = 10 if method == "tools/list" else 3
            data = json.dumps(request).encode('utf-8')
            req = urllib.request.Request(
                self.mcp_url,
                data=data,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json, text/event-stream',
                    'Connection': 'close'  # Don't keep connections open
                }
            )

            logging.debug(f"Using timeout of {timeout}s for method {method}")
            
            with urllib.request.urlopen(req, timeout=timeout) as response:
                # Handle SSE response directly from the response object
                content_type = response.headers.get('Content-Type', '')
                
                if 'text/event-stream' in content_type:
                    logging.debug("Handling SSE response")
                    parsed_response = self.parse_sse_response(response, request_id)
                else:
                    logging.debug("Handling JSON response")
                    response_text = response.read().decode('utf-8')
                    try:
                        parsed_response = json.loads(response_text)
                        if 'id' not in parsed_response or parsed_response['id'] is None:
                            parsed_response['id'] = request_id
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse JSON response: {e}")
                        parsed_response = {
                            "jsonrpc": "2.0",
                            "error": {
                                "code": -32603,
                                "message": f"Invalid JSON response: {str(e)}"
                            },
                            "id": request_id
                        }
                
                # Log server response for debugging
                if "error" in parsed_response:
                    logging.error(f"SERVER ERROR - Request {request_id} ({method}): {parsed_response}")
                elif method == "tools/call":
                    logging.info(f"TOOL CALL - Server response: {str(parsed_response)[:500]}...")
                
                elapsed = time.time() - start_time
                logging.debug(f"Request {request_id} completed in {elapsed:.2f}s")
                return parsed_response

        except urllib.error.HTTPError as e:
            elapsed = time.time() - start_time
            error_message = f"HTTP {e.code}: {e.reason}"
            try:
                error_body = e.read().decode('utf-8')
                error_message += f" - {error_body}"
            except:
                pass

            logging.error(f"HTTP error for request {request_id} after {elapsed:.2f}s: {error_message}")
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": e.code,
                    "message": error_message
                },
                "id": request_id
            }

        except urllib.error.URLError as e:
            elapsed = time.time() - start_time
            error_msg = f"Connection error: {str(e)}"
            logging.error(f"URL error for request {request_id} after {elapsed:.2f}s: {error_msg}")
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": error_msg
                },
                "id": request_id
            }

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = f"Unexpected error: {str(e)}"
            logging.error(f"Unexpected error for request {request_id} after {elapsed:.2f}s: {error_msg}")
            return {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": error_msg
                },
                "id": request_id
            }

    def safe_print(self, data):
        """Print with improved error handling and immediate flushing"""
        try:
            output = json.dumps(data)
            print(output, flush=True)  # Use flush=True for immediate output
            logging.debug(f"Successfully sent response: {len(output)} chars")

        except BrokenPipeError:
            logging.error("Broken pipe error - client disconnected")
            sys.exit(0)

        except Exception as e:
            logging.error(f"Print error: {e}")
            # Don't exit on print errors, just log them

    def process_request(self, line: str) -> None:
        """Process a single request line with improved error handling"""
        try:
            request = json.loads(line)
            request_id = request.get("id", "unknown")
            method = request.get("method", "unknown")

            logging.debug(f"Processing request {request_id}: {method}")

            # Validate required fields
            if not isinstance(request, dict):
                raise ValueError("Request must be a JSON object")

            if "method" not in request:
                raise ValueError("Request must have a 'method' field")

            # Handle notifications/initialized locally (Malloy server doesn't support it)
            if method == "notifications/initialized":
                logging.info(f"Handling notifications/initialized locally for request {request_id}")
                # For notifications, we don't send a response (notifications are one-way)
                return

            # Ensure ID is present and valid
            if "id" not in request:
                request["id"] = 1  # Default ID
            elif request["id"] is None:
                request["id"] = 1  # Replace null with default

            # Send request and get response
            response = self.send_request(request)

            # Send response immediately
            self.safe_print(response)

        except json.JSONDecodeError as e:
            logging.error(f"JSON parse error: {e}")
            error_response = {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32700,
                    "message": f"Parse error: {str(e)}"
                },
                "id": None
            }
            self.safe_print(error_response)

        except ValueError as e:
            logging.error(f"Request validation error: {e}")
            error_response = {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32600,
                    "message": f"Invalid Request: {str(e)}"
                },
                "id": None
            }
            self.safe_print(error_response)

        except Exception as e:
            logging.error(f"Unexpected error processing request: {e}")
            error_response = {
                "jsonrpc": "2.0",
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                },
                "id": None
            }
            self.safe_print(error_response)

    def run(self):
        """Main loop with improved stdin handling"""
        logging.info("Starting main processing loop")

        try:
            # Process stdin line by line with immediate handling
            for line in sys.stdin:
                line = line.strip()
                if not line:
                    continue

                # Process each request immediately
                self.process_request(line)

        except KeyboardInterrupt:
            logging.info("Received keyboard interrupt, shutting down")
            sys.exit(0)

        except BrokenPipeError:
            logging.info("Broken pipe detected, client disconnected")
            sys.exit(0)

        except Exception as e:
            logging.error(f"Fatal error in main loop: {e}")
            sys.exit(1)

        logging.info("Main loop completed")

if __name__ == "__main__":
    try:
        bridge = ImprovedMalloyMCPBridge()
        bridge.run()
    except Exception as e:
        logging.error(f"Failed to start bridge: {e}")
        sys.exit(1)
