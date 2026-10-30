"""Synchronous MCP metadata discovery; no tool invocation."""
import json
import os
import ssl
import urllib.error
import urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

class MCP:
    """Small Streamable HTTP probe for JSON/SSE, without invoking live tools.

    Implements the 2025-03-26 initialization/session headers. Not a full SDK.
    No background GET stream is needed for the synchronous probes in this suite.
    """
    def __init__(self, url, authorization=None, ca_file=None):
        self.url = url
        self.authorization = authorization
        self.context = ssl.create_default_context(cafile=ca_file or os.getenv("CA_BUNDLE") or None)
        self.opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=self.context), NoRedirect())
        self.session = None
        self.version = "2025-03-26"
        self.sequence = 0

    def request(self, method, params=None, notification=False):
        self.sequence += 1
        body = {"jsonrpc": "2.0", "method": method}
        if not notification:
            body["id"] = self.sequence
        if params is not None:
            body["params"] = params
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
        if self.authorization:
            headers["Authorization"] = self.authorization
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        if method != "initialize":
            headers["MCP-Protocol-Version"] = self.version
        req = urllib.request.Request(self.url, json.dumps(body).encode(), headers)
        try:
            with self.opener.open(req, timeout=20) as res:
                status, content_type = res.status, res.headers.get("Content-Type", "")
                if res.headers.get("Mcp-Session-Id"):
                    self.session = res.headers["Mcp-Session-Id"]
                if "text/event-stream" in content_type:
                    # Read one matching response, not an indefinitely open SSE stream.
                    data = []
                    payload = None
                    for raw in res:
                        line = raw.decode().rstrip("\r\n")
                        if line.startswith("data:"):
                            data.append(line[5:].lstrip())
                        elif not line and data:
                            candidate = json.loads("\n".join(data))
                            data = []
                            if candidate.get("id") == body.get("id"):
                                payload = candidate
                                break
                else:
                    raw = res.read(2_000_000)
                    payload = json.loads(raw) if raw else None
                return {"http_status": status, "body": payload}
        except urllib.error.HTTPError as e:
            # Error bodies may contain protected details; persist no raw error body.
            return {"http_status": e.code, "body": None}

    def initialize(self):
        r = self.request("initialize", {"protocolVersion": self.version, "capabilities": {},
                                        "clientInfo": {"name": "acceptance-validator", "version": "1.0"}})
        if r["http_status"] == 200 and (r.get("body") or {}).get("result"):
            self.version = r["body"]["result"]["protocolVersion"]
            self.request("notifications/initialized", notification=True)
        return r

    def list_all(self, method, key):
        items, seen, cursor = [], set(), None
        for _ in range(100):
            r = self.request(method, {"cursor": cursor} if cursor else {})
            b = r.get("body") or {}
            if r["http_status"] != 200 or "error" in b or "result" not in b:
                return {"http_status": r["http_status"], "error": b.get("error"), "items": items, "complete": False}
            items += b["result"].get(key, [])
            cursor = b["result"].get("nextCursor")
            if not cursor:
                return {"http_status": 200, "items": items, "complete": True}
            if cursor in seen:
                raise RuntimeError("Repeated pagination cursor")
            seen.add(cursor)
        raise RuntimeError("Pagination exceeded 100 pages")

    def close(self):
        if self.session:
            headers = {"Mcp-Session-Id": self.session, "MCP-Protocol-Version": self.version}
            if self.authorization:
                headers["Authorization"] = self.authorization
            try:
                self.opener.open(urllib.request.Request(self.url, method="DELETE", headers=headers), timeout=5).close()
            except (urllib.error.URLError, TimeoutError):
                pass
