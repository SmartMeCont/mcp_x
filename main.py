import os
import asyncio
import json
import logging
import sys
import threading
import sqlite3
import socket
import ipaddress
import urllib.request
from urllib.parse import urlparse
from datetime import datetime, timedelta
from http.server import SimpleHTTPRequestHandler, HTTPServer
import websockets
from mcp.server.fastmcp import FastMCP
from ddgs import DDGS
from bs4 import BeautifulSoup

# =====================================================================
# CONFIGURATION: Reads your endpoint securely from Environment Secrets
# =====================================================================
MCP_BRIDGE_ENDPOINT = os.environ.get("MCP_BRIDGE_ENDPOINT")
REMINDERS_FILE = "reminders.json"
MEMORY_DB = "long_term_memory.db"
MAX_FETCH_BYTES = 1_000_000  # cap on bytes read from any single scraped page

# Setup clean logging output
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("McpBridgeClient")

# Initialize the Unified MCP Instance
mcp = FastMCP("AdvancedUnifiedToolbox")

# =====================================================================
# SYSTEM SETUP: SQLITE MEMORY DATABASE
# =====================================================================
def init_memory_db():
    conn = sqlite3.connect(MEMORY_DB)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT NOT NULL,
            category TEXT,
            timestamp TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()

init_memory_db()

# Helper functions for the local Calendar database
def load_reminders():
    if not os.path.exists(REMINDERS_FILE):
        return []
    try:
        with open(REMINDERS_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load reminders file, starting empty: {e}")
        return []

def save_reminders(reminders):
    # Write to a temp file then atomically replace, so a crash mid-write
    # can't corrupt the reminders store.
    tmp_path = f"{REMINDERS_FILE}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(reminders, f, indent=4)
    os.replace(tmp_path, REMINDERS_FILE)

# =====================================================================
# TOOL CATEGORY 1: CALCULATOR
# =====================================================================
@mcp.tool()
def add(a: float, b: float) -> float:
    """Add two numbers together."""
    return a + b

@mcp.tool()
def subtract(a: float, b: float) -> float:
    """Subtract b from a."""
    return a - b

@mcp.tool()
def multiply(a: float, b: float) -> float:
    """Multiply two numbers."""
    return a * b

@mcp.tool()
def divide(a: float, b: float) -> str:
    """Divide a by b. Safely checks for zero."""
    if b == 0:
        return "Error: Cannot divide by zero."
    return str(a / b)

# =====================================================================
# TOOL CATEGORY 2: SMART CALENDAR & REMINDERS
# =====================================================================
@mcp.tool()
def add_reminder(text: str, date_time: str) -> str:
    """Saves an alarm reminder or calendar event. Format: 'YYYY-MM-DD HH:MM' (e.g., '2026-10-25 14:30')."""
    try:
        datetime.strptime(date_time, "%Y-%m-%d %H:%M")
        reminders = load_reminders()
        reminders.append({
            "text": text,
            "time": date_time,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M")
        })
        save_reminders(reminders)
        return f"Success: Reminder set for '{text}' on {date_time}."
    except ValueError:
        return "Error: Please use the strict format 'YYYY-MM-DD HH:MM'."

@mcp.tool()
def query_reminders(timeframe: str = "all") -> str:
    """
    Retrieves and filters active reminders. 
    Allowed timeframe options: 'all', 'today', 'week'.
    """
    reminders = load_reminders()
    if not reminders:
        return "Your calendar is empty. No reminders found."
    
    now = datetime.now()
    today_date = now.date()
    end_of_week = today_date + timedelta(days=6)  # 7-day inclusive window (today..+6)
    
    filtered = []
    for r in reminders:
        try:
            rem_dt = datetime.strptime(r['time'], "%Y-%m-%d %H:%M")
            rem_date = rem_dt.date()
            
            if timeframe == "today" and rem_date == today_date:
                filtered.append(r)
            elif timeframe == "week" and today_date <= rem_date <= end_of_week:
                filtered.append(r)
            elif timeframe == "all":
                filtered.append(r)
        except Exception:
            if timeframe == "all":
                filtered.append(r)

    if not filtered:
        return f"No reminders found for the timeframe: '{timeframe}'."

    filtered.sort(key=lambda x: x.get('time', ''))
    
    result = [f"--- Calendar Reminders [Filter: {timeframe.upper()}] (Current Server Time: {now.strftime('%Y-%m-%d %H:%M')}) ---"]
    for i, r in enumerate(filtered, 1):
        result.append(f"{i}. [{r['time']}] - {r['text']}")
    return "\n".join(result)

# =====================================================================
# TOOL CATEGORY 3: QUICK WEB SEARCH
# =====================================================================
@mcp.tool()
async def web_search(query: str, max_results: int = 5) -> str:
    """
    Performs a quick web search and returns short result snippets (title, URL,
    summary) without visiting or scraping any pages. Use this for simple,
    fast lookups such as current prices, scores, quick facts, or definitions.
    For in-depth topics that need multi-source analysis and full-page content,
    use 'deep_research' instead.
    """
    def _run_search() -> str:
        logger.info(f"Performing quick web search for: {query}")
        try:
            with DDGS() as ddgs:
                search_results = ddgs.text(query, max_results=max(1, min(max_results, 10)))
                if not search_results:
                    return f"No web search results found for: '{query}'."

                lines = [f"--- Web Search Results for: {query} ---"]
                for index, res in enumerate(search_results, 1):
                    title = res.get('title', 'Unknown Source')
                    href = res.get('href', 'N/A')
                    snippet = res.get('body', '')
                    lines.append(f"{index}. {title}\n   URL: {href}\n   {snippet}")
                return "\n".join(lines)
        except Exception as e:
            return f"Error executing web search: {str(e)}"

    # Run the blocking network call in a worker thread so it can never stall
    # the websocket bridge's event loop (and its ping/pong keepalive).
    return await asyncio.to_thread(_run_search)

# =====================================================================
# SECURITY HELPER: Basic SSRF guard for outbound scraping
# =====================================================================
def _is_safe_url(url: str) -> bool:
    """Only allow http(s) URLs that don't resolve to private/internal/loopback
    addresses, to reduce SSRF risk when following third-party search results."""
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            return False
        for info in socket.getaddrinfo(parsed.hostname, None):
            ip = ipaddress.ip_address(info[4][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast:
                return False
        return True
    except Exception:
        return False

# =====================================================================
# TOOL CATEGORY 4: DEEP RESEARCH & THINKING AGENT
# =====================================================================
@mcp.tool()
async def deep_research(topic: str) -> str:
    """
    Performs full deep research for topics that genuinely require it (e.g.
    in-depth explanations, comparisons, analysis, or open-ended questions).
    Searches the live web, visits and scrapes text contents from multiple
    top target sites, analyzes them, and synthesizes a deep response. This
    is slower than 'web_search' (it scrapes full pages), so do NOT use it for
    simple quick-fact lookups (e.g. current prices, scores, dates) — use
    'web_search' for those instead.
    """
    def _run_deep_research() -> str:
        logger.info(f"Initiating deep multi-source research for: {topic}")
        try:
            with DDGS() as ddgs:
                search_results = ddgs.text(topic, max_results=3)
                if not search_results:
                    return "Deep research failed: No initial search results found."

                compiled_knowledge = []

                for index, res in enumerate(search_results, 1):
                    url = res.get('href')
                    title = res.get('title', 'Unknown Source')
                    snippet = res.get('body', '')

                    if not url or not _is_safe_url(url):
                        body_text = f"[Skipped unsafe or invalid URL. Falling back to snippet]: {snippet}"
                    else:
                        try:
                            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                            with urllib.request.urlopen(req, timeout=5) as response:
                                html = response.read(MAX_FETCH_BYTES)
                                soup = BeautifulSoup(html, 'html.parser')
                                paragraphs = [p.get_text() for p in soup.find_all('p')]
                                body_text = " ".join(paragraphs)[:2500]
                        except Exception:
                            body_text = f"[Could not deep scrape full URL. Falling back to snippet]: {snippet}"

                    compiled_knowledge.append(f"### SOURCE {index}: {title}\nURL: {url}\nDEEP MATERIAL:\n{body_text}\n")

                report_header = f"=== DEEP RESEARCH REPORT GENERATED FOR: {topic.upper()} ===\n"
                return report_header + "\n".join(compiled_knowledge)

        except Exception as e:
            return f"Error executing deep research module: {str(e)}"

    # Run the blocking network/scraping work in a worker thread so a slow
    # multi-site fetch can't stall the websocket bridge's event loop.
    return await asyncio.to_thread(_run_deep_research)

# =====================================================================
# TOOL CATEGORY 5: LONG TERM MEMORY VAULT
# =====================================================================
@mcp.tool()
def save_to_memory(fact_or_context: str, category: str = "general") -> str:
    """Stores important contextual information, user preferences, or conversation facts into long-term memory."""
    try:
        conn = sqlite3.connect(MEMORY_DB)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO memories (content, category, timestamp) VALUES (?, ?, ?)",
            (fact_or_context, category, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )
        conn.commit()
        conn.close()
        return f"Success: Stored info into long-term memory under category '{category}'."
    except Exception as e:
        return f"Memory storage failed: {str(e)}"

@mcp.tool()
def search_memory(query_keyword: str) -> str:
    """Searches long-term memory history for matching keywords, preferences, or topics previously discussed."""
    try:
        conn = sqlite3.connect(MEMORY_DB)
        cursor = conn.cursor()
        cursor.execute("SELECT content, category, timestamp FROM memories WHERE content LIKE ?", (f"%{query_keyword}%",))
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return f"No long-term memories found matching the keyword: '{query_keyword}'."
            
        memories = ["=== Recovered Long-Term Memories ==="]
        for row in rows:
            memories.append(f"• [{row[2]}] ({row[1]}): {row[0]}")
        return "\n".join(memories)
    except Exception as e:
        return f"Memory retrieval failed: {str(e)}"

# =====================================================================
# CORE PIPELINE & HOSTING BRIDGE (Koyeb compatible)
# =====================================================================
async def list_available_tools():
    """Returns tool metadata using the public FastMCP API when available,
    falling back to older internal storage for compatibility."""
    if hasattr(mcp, "list_tools"):
        tools = await mcp.list_tools()
        return [
            {
                "name": t.name,
                "description": getattr(t, "description", "") or "",
                "inputSchema": getattr(t, "inputSchema", None) or getattr(t, "input_schema", {}) or {}
            }
            for t in tools
        ]
    # Legacy fallback for older FastMCP versions without list_tools()
    return [
        {
            "name": t.name,
            "description": getattr(t, "description", "") or "",
            "inputSchema": getattr(t, "input_schema", None) or getattr(t, "inputSchema", {}) or {}
        }
        for t in mcp._tools.values()
    ]

async def execute_tool(tool_name: str, tool_args: dict):
    """Executes a registered tool by name, preferring the public FastMCP
    call_tool API (which validates arguments and offloads sync functions off
    the event loop). Falls back to legacy internal dict access for older SDK
    versions, running the sync tool function in a worker thread so a slow
    tool (e.g. deep_research) can't stall the websocket's ping/pong."""
    if hasattr(mcp, "call_tool"):
        try:
            return await mcp.call_tool(tool_name, tool_args)
        except Exception as e:
            if "not found" in str(e).lower() or "unknown tool" in str(e).lower():
                raise LookupError(f"Tool {tool_name} not found") from e
            raise

    if tool_name not in mcp._tools:
        raise LookupError(f"Tool {tool_name} not found")
    tool_fn = mcp._tools[tool_name]
    if asyncio.iscoroutinefunction(tool_fn):
        return await tool_fn(**tool_args)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: tool_fn(**tool_args))

def _extract_text(result) -> str:
    """Normalizes whatever shape call_tool/legacy invocation returns into text."""
    # Newer SDKs may wrap results in an object exposing a `.content` list
    # (e.g. CallToolResult) rather than returning the list directly.
    content = getattr(result, "content", None)
    items = content if content is not None else result

    if isinstance(items, (list, tuple)):
        parts = []
        for item in items:
            text = getattr(item, "text", None)
            parts.append(text if text is not None else str(item))
        return "\n".join(parts)
    return str(items)

async def run_mcp_bridge(endpoint_url: str):
    logger.info("Connecting directly to remote MCP Bridge Endpoint...")
    while True:
        try:
            async for websocket in websockets.connect(endpoint_url, ping_interval=20, ping_timeout=10):
                try:
                    logger.info("Successfully connected to the remote cloud server!")
                    async for message in websocket:
                        request = json.loads(message)
                        method = request.get("method")
                        req_id = request.get("id")
                        
                        if method == "initialize":
                            params = request.get("params", {})
                            response = {"jsonrpc": "2.0","id": req_id,"result": {"protocolVersion": params.get("protocolVersion", "2024-11-05"),"capabilities": {"tools": {"listChanged": False}},"serverInfo": {"name": "AdvancedUnifiedToolbox","version": "1.0.0"}}}
                            await websocket.send(json.dumps(response))
                            logger.info("MCP initialize handshake completed.")
                        elif method == "notifications/initialized":
                            logger.info("MCP client initialization completed.")
                        elif method == "tools/list":
                            try:
                                tools_list = await list_available_tools()
                                response = {"jsonrpc": "2.0", "id": req_id, "result": {"tools": tools_list}}
                            except Exception as e:
                                logger.exception("Failed to list tools")
                                response = {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32603, "message": str(e)}}
                            await websocket.send(json.dumps(response))
                            logger.info("Synchronized structural multi-tool suite definitions.")
                        elif method == "tools/call":
                            params = request.get("params", {})
                            tool_name = params.get("name")
                            tool_args = params.get("arguments", {})

                            logger.info(f"Execution requested for tool: {tool_name} with args: {tool_args}")

                            try:
                                result = await execute_tool(tool_name, tool_args)
                                response = {
                                    "jsonrpc": "2.0",
                                    "id": req_id,
                                    "result": {
                                        "content": [{"type": "text", "text": _extract_text(result)}]
                                    }
                                }
                            except LookupError as e:
                                response = {
                                    "jsonrpc": "2.0",
                                    "id": req_id,
                                    "error": {"code": -32601, "message": str(e)}
                                }
                            except Exception as e:
                                logger.exception(f"Tool execution failed for '{tool_name}'")
                                response = {
                                    "jsonrpc": "2.0",
                                    "id": req_id,
                                    "error": {"code": -32603, "message": str(e)}
                                }

                            await websocket.send(json.dumps(response))
                            logger.info(f"Returned execution results for '{tool_name}'.")

                except websockets.ConnectionClosed:
                    logger.warning("Connection lost inside session. Attempting to reconnect...")
                except Exception as e:
                    logger.error(f"Internal processing loop error occurred: {e}")

        except Exception as e:
            logger.error(f"Failed to connect or connection lost completely: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

def run_health_check_server():
    """Starts a minimal HTTP server to satisfy Hugging Face and allow pinging."""
    class HealthCheckHandler(SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, format, *args):
            return

    # Port Dynamic
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)

    logger.info(f"Health check server started on port {port}")
    server.serve_forever()

if __name__ == "__main__":
    if not MCP_BRIDGE_ENDPOINT:
        logger.error("Error: MCP_BRIDGE_ENDPOINT variable not found in Secrets configuration!")
        sys.exit(1)

    logger.warning(
        f"Reminders and long-term memory are stored on local disk ({REMINDERS_FILE}, {MEMORY_DB}); "
        "data will be lost on redeploy unless persistent storage is configured."
    )

    # Start the HTTP server in a separate background thread so it doesn't block the WebSocket bridge
    threading.Thread(target=run_health_check_server, daemon=True).start()

    try:
        asyncio.run(run_mcp_bridge(MCP_BRIDGE_ENDPOINT))
    except KeyboardInterrupt:
        logger.info("Shutting down All-in-One Multi-Tool MCP Server.")
