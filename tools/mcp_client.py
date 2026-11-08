#!/usr/bin/env python3
"""
Minimal MCP (Model Context Protocol) client over streamable-HTTP transport.

Enough of the protocol to initialize a session, list tools, and call them.
Used to query the Chargebee data-lookup MCP server without needing the tools
loaded into a Claude Code session.

Credentials come from the environment or a .env file in the repo root:
    CHARGEBEE_MCP_URL
    CHARGEBEE_MCP_KEY
"""
import json
import os
import sys
import urllib.error
import urllib.request

TIMEOUT = 180


def load_dotenv(path):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def parse_body(raw, content_type):
    """Server may answer as plain JSON or as an SSE stream of `data:` frames."""
    text = raw.decode("utf-8", "replace")
    if "text/event-stream" in (content_type or ""):
        messages = []
        for line in text.splitlines():
            if line.startswith("data:"):
                chunk = line[5:].strip()
                if chunk and chunk != "[DONE]":
                    try:
                        messages.append(json.loads(chunk))
                    except json.JSONDecodeError:
                        pass
        # The reply to our request is the last frame carrying a result/error.
        for msg in reversed(messages):
            if "result" in msg or "error" in msg:
                return msg
        return messages[-1] if messages else None
    if not text.strip():
        return None
    return json.loads(text)


class MCPClient:
    def __init__(self, url, api_key=None, verbose=False):
        self.url = url
        self.api_key = api_key
        self.session_id = None
        self.next_id = 0
        self.verbose = verbose

    def _headers(self):
        h = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "MCP-Protocol-Version": "2025-06-18",
        }
        if self.api_key:
            h["Authorization"] = "Bearer %s" % self.api_key
        if self.session_id:
            h["Mcp-Session-Id"] = self.session_id
        return h

    def _post(self, payload):
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(self.url, data=data, method="POST")
        for k, v in self._headers().items():
            req.add_header(k, v)
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                # The server assigns a session id on initialize; reuse it after.
                sid = resp.headers.get("Mcp-Session-Id")
                if sid:
                    self.session_id = sid
                return parse_body(resp.read(), resp.headers.get("Content-Type"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")[:1000]
            raise SystemExit("MCP HTTP %d from %s\n%s" % (e.code, self.url, body))
        except urllib.error.URLError as e:
            raise SystemExit("Could not reach MCP server %s: %s" % (self.url, e))

    def request(self, method, params=None):
        self.next_id += 1
        payload = {"jsonrpc": "2.0", "id": self.next_id, "method": method}
        if params is not None:
            payload["params"] = params
        if self.verbose:
            sys.stderr.write("-> %s\n" % method)
        resp = self._post(payload)
        if resp and "error" in resp:
            raise SystemExit("MCP error on %s: %s" % (method, json.dumps(resp["error"])))
        return (resp or {}).get("result")

    def notify(self, method, params=None):
        payload = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            payload["params"] = params
        self._post(payload)

    def initialize(self):
        result = self.request(
            "initialize",
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "s3-log-combiner-report", "version": "1.0.0"},
            },
        )
        self.notify("notifications/initialized")
        return result

    def list_tools(self):
        return (self.request("tools/list") or {}).get("tools", [])

    def call_tool(self, name, arguments):
        return self.request("tools/call", {"name": name, "arguments": arguments})


def text_of(tool_result):
    """Flatten an MCP tool result into plain text."""
    if not tool_result:
        return ""
    parts = []
    for item in tool_result.get("content", []) or []:
        if item.get("type") == "text":
            parts.append(item.get("text", ""))
    if not parts and "structuredContent" in tool_result:
        return json.dumps(tool_result["structuredContent"], indent=1)
    return "\n".join(parts)


def connect_chargebee(verbose=False):
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(repo_root, ".env"))
    url = os.environ.get("CHARGEBEE_MCP_URL")
    key = os.environ.get("CHARGEBEE_MCP_KEY")
    if not url:
        raise SystemExit("Set CHARGEBEE_MCP_URL (and CHARGEBEE_MCP_KEY) in .env")
    client = MCPClient(url, key, verbose=verbose)
    client.initialize()
    return client


if __name__ == "__main__":
    client = connect_chargebee(verbose=True)
    tools = client.list_tools()
    print("Connected. %d tool(s):\n" % len(tools))
    for t in tools:
        print("=" * 70)
        print("NAME: %s" % t.get("name"))
        print("DESC: %s" % (t.get("description") or "").strip())
        schema = t.get("inputSchema") or {}
        print("INPUT SCHEMA:\n%s" % json.dumps(schema, indent=1))
