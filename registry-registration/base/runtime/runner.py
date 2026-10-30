#!/usr/bin/env python3
"""Standalone, standard-library registry publisher for an Argo CD hook Job.

Only registration/metadata API operations; never changes an MCP deployment,
invokes runtime capabilities, or deletes registry records.
"""
import argparse
import hashlib
import json
import os
import re
import ssl
import string
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Registry:
    def __init__(self, settings):
        self.workspace = settings["REGISTRY_WORKSPACE"]
        self.url = settings["REGISTRY_URL"].rstrip("/")
        if urllib.parse.urlsplit(self.url).scheme != "https":
            raise ValueError("REGISTRY_URL must use HTTPS")
        self.token_file = Path(settings["REGISTRY_TOKEN_FILE"])
        ca_file = Path(settings["REGISTRY_CA_FILE"])
        # OpenShift service CA injection can finish shortly after sync.
        for attempt in range(20):
            if ca_file.exists() and "BEGIN CERTIFICATE" in ca_file.read_text():
                break
            if attempt == 19:
                raise RuntimeError("Registry trust bundle has not been injected")
            time.sleep(1)
        context = ssl.create_default_context(cafile=str(ca_file))
        self.opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), NoRedirect())

    @staticmethod
    def path(name, version=None):
        path = "/" + urllib.parse.quote(name, safe="")
        return path + "/versions/" + urllib.parse.quote(version, safe="") if version else path

    def request(self, method, path="", body=None):
        # Read the projected token on every request so kubelet rotation works.
        token = self.token_file.read_text().strip()
        req = urllib.request.Request(self.url + "/api/3.0/mlflow/mcp-servers" + path,
            json.dumps(body).encode() if body is not None else None,
            {"Authorization": "Bearer " + token, "X-MLFLOW-WORKSPACE": self.workspace,
             "Content-Type": "application/json"}, method=method)
        try:
            with self.opener.open(req, timeout=20) as response:
                raw = response.read(8_000_000)
                return response.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            # No raw bodies, redirect targets, authorization headers or metadata
            # values enter logs. Never forward the token through redirects.
            try:
                code = json.loads(exc.read(100_000)).get("error_code", "HTTP_ERROR")
            except (ValueError, AttributeError):
                code = "HTTP_ERROR"
            return exc.code, {"error_code": code}


def enabled(settings, key, default="false"):
    value = settings.get(key, default).lower()
    if value not in {"true", "false"}:
        raise ValueError(key + " must be true or false")
    return value == "true"


def render(value, settings):
    if isinstance(value, dict):
        return {k: render(v, settings) for k,v in value.items()}
    if isinstance(value, list):
        return [render(v, settings) for v in value]
    return string.Template(value).substitute(settings) if isinstance(value, str) else value


def load(path, settings):
    return render(json.loads(path.read_text()), settings)


def resolve_route(settings):
    """Resolve a controller-assigned Route host; never store cluster hosts in Git."""
    name = settings.get("MCP_ROUTE_NAME")
    if not name:
        return
    namespace = settings["POD_NAMESPACE"]
    host = settings.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    if ":" in host and not host.startswith("["):
        host = "[" + host + "]"
    port = settings.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    context = ssl.create_default_context(cafile=settings.get("KUBERNETES_CA_FILE", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"))
    opener = urllib.request.build_opener(urllib.request.HTTPSHandler(context=context), NoRedirect())
    url = f"https://{host}:{port}/apis/route.openshift.io/v1/namespaces/{urllib.parse.quote(namespace,safe='')}/routes/{urllib.parse.quote(name,safe='')}"
    deadline = time.monotonic() + int(settings.get("MCP_WAIT_SECONDS", "180"))
    while True:
        token = Path(settings["REGISTRY_TOKEN_FILE"]).read_text().strip()
        req = urllib.request.Request(url, headers={"Authorization":"Bearer "+token})
        with opener.open(req, timeout=20) as response:
            route = json.loads(response.read())
        hostname = route.get("spec", {}).get("host")
        admitted = any(c.get("type")=="Admitted" and c.get("status")=="True"
                       for ingress in route.get("status",{}).get("ingress",[]) for c in ingress.get("conditions",[]))
        if hostname and admitted:
            if not re.fullmatch(r"[A-Za-z0-9.-]+",hostname):
                raise ValueError("Route returned an invalid host")
            settings["MCP_URL"] = "https://" + hostname + settings.get("MCP_PATH", "/mcp")
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("MCP Route is not admitted; no registry writes performed")
        time.sleep(2)


def discover(server, settings, initialize_only=False):
    from mcp_http import MCP
    mode = settings.get("MCP_AUTH_MODE", "serviceaccount")
    if mode == "serviceaccount":
        auth = "Bearer " + Path(settings["REGISTRY_TOKEN_FILE"]).read_text().strip()
    elif mode == "header":
        auth = settings["MCP_AUTHORIZATION"]  # Optional Secret-backed env injection.
    elif mode == "none":
        auth = None
    else:
        raise ValueError("MCP_AUTH_MODE must be serviceaccount, header or none")
    url = settings.get("MCP_DISCOVERY_URL") or settings["MCP_URL"]
    deadline = time.monotonic() + int(settings.get("MCP_WAIT_SECONDS", "0"))
    while True:
        client = MCP(url, auth, ca_file=settings.get("MCP_CA_FILE") or None)
        try:
            init = client.initialize()
            if init["http_status"] in {404,502,503,504} and time.monotonic() < deadline:
                client.close()
                time.sleep(2)
                continue
            break
        except (urllib.error.URLError, TimeoutError, OSError):
            if time.monotonic() >= deadline:
                raise RuntimeError("MCP runtime is not reachable; no registry writes performed") from None
            time.sleep(2)
    try:
        result = (init.get("body") or {}).get("result")
        if not result:
            raise RuntimeError("MCP metadata initialization failed, HTTP " + str(init["http_status"]))
        if result.get("serverInfo", {}).get("version") != server["version"]:
            raise RuntimeError("SERVER_VERSION differs from the running MCP server; no registration writes performed")
        if initialize_only:
            return result["protocolVersion"], None
        snapshots = {}
        for key, method, field in (("tools", "tools/list", "tools"),
                                   ("resources", "resources/list", "resources"),
                                   ("prompts", "prompts/list", "prompts"),
                                   ("resourceTemplates", "resources/templates/list", "resourceTemplates")):
            capability = "resources" if key == "resourceTemplates" else key
            if capability not in result.get("capabilities", {}):
                snapshots[key] = []
                continue
            got = client.list_all(method, field)
            if not got["complete"]:
                raise RuntimeError("Incomplete MCP " + key + " snapshot; no registration writes performed")
            snapshots[key] = sorted(got["items"], key=lambda item: item.get("uri", item.get("name", "")))
        return result["protocolVersion"], snapshots
    finally:
        client.close()


def build_plans(settings, data_dir, live=False):
    plans = []
    if enabled(settings, "REGISTER_REAL", "true"):
        for key in ("MCP_OWNER", "MCP_SECURITY_DOMAIN", "MCP_SUPPORT_CONTACT"):
            if not settings.get(key) or settings[key].startswith("replace-with-"):
                raise ValueError("Set " + key + " in the cluster overlay")
        server = load(data_dir / "server.json", settings)
        metadata = {k:str(v) for k,v in load(data_dir / "metadata.json", settings).items()}
        tools = None
        if settings.get("CAPABILITIES_FILE"):
            snapshot = json.loads(Path(settings["CAPABILITIES_FILE"]).read_text())
            if snapshot["serverInfo"]["version"] != server["version"]:
                raise ValueError("Committed capability snapshot version differs from SERVER_VERSION")
            tools = snapshot["tools"]
            server.setdefault("_meta", {})["governance.example.com"] = {
                key:snapshot[key] for key in ("resources", "resourceTemplates", "prompts")}
            metadata["protocol_version"] = snapshot["protocolVersion"]
            if live and enabled(settings, "DISCOVER_REAL", "true"):
                protocol, _ = discover(server,settings,initialize_only=True)
                if protocol != snapshot["protocolVersion"]:
                    raise RuntimeError("Runtime protocol differs from the committed snapshot")
        elif live and enabled(settings, "DISCOVER_REAL", "true"):
            protocol, snapshot = discover(server, settings)
            metadata["protocol_version"] = protocol
            tools = snapshot.pop("tools")
            server.setdefault("_meta", {})["governance.example.com"] = snapshot
        if settings.get("MCP_ROUTE_NAME"):
            metadata["route_reference"] = "Route/" + settings["MCP_NAMESPACE"] + "/" + settings["MCP_ROUTE_NAME"]
        plans.append({"name":server["name"], "description":server.get("description", ""),
                      "versions":[{"server_json":server, "tools":tools, "metadata":metadata,
                                   "status":settings.get("REAL_VERSION_STATUS", "draft")} ]})
    if enabled(settings, "REGISTER_POC"):
        name = settings["POC_RECORD_NAME"]
        if not name.startswith("io.example/mcp-acceptance-"):
            raise ValueError("POC_RECORD_NAME must start io.example/mcp-acceptance-")
        spec = load(data_dir / "poc-versions.json", settings)
        versions = []
        for entry in spec["versions"]:
            version = str(entry["version"])
            versions.append({"server_json":{"name":name, "version":version, "title":entry["title"],
                "description":entry["description"], "remotes":[{"type":"streamable-http", "url":"https://mcp.example.invalid/mcp"}],
                "_meta":{"governance.example.com":{"resources":[], "prompts":[], "capability_snapshot":"synthetic-poc-"+version}}},
                "tools":entry.get("tools"), "status":entry.get("status", "draft"),
                "metadata":{**{k:str(v) for k,v in spec.get("version_metadata", {}).items()},
                            "project":settings["REGISTRY_WORKSPACE"], "capabilities":"synthetic-poc-"+version}})
        plans.append({"name":name, "description":spec["server"]["description"], "versions":versions})
    if not plans:
        raise ValueError("Enable REGISTER_REAL and/or REGISTER_POC")
    return plans


def tools_equal(got, want):
    if want is None:
        return got is None
    return isinstance(got, list) and len(got) == len(want) and all(
        {k:v for k,v in actual.items() if v is not None or k in expected} == expected
        for actual,expected in zip(got,want))


def version_equal(got, item):
    return (got.get("server_json") == item["server_json"] and tools_equal(got.get("tools"), item.get("tools"))
            and all(got.get("tags", {}).get(k) == v for k,v in item["metadata"].items()))


def source_for(item, application):
    # Exclude status: Git may promote this application's own draft to active.
    payload = {k:item[k] for k in ("server_json", "tools", "metadata")}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    return "gitops:" + application + ":" + digest


def reconcile(registry, plans, application):
    actions, servers = [], []
    seen = set()
    # Validate/read ALL records before the first write, including optional POC data.
    for plan in plans:
        name = plan["name"]
        if not re.fullmatch(r"[a-z0-9]+(?:[.-][a-z0-9]+)+/[a-zA-Z0-9._-]+", name):
            raise ValueError("Invalid registry name")
        code, server = registry.request("GET", registry.path(name))
        if code not in {200,404} or code == 200 and server.get("name") != name:
            raise RuntimeError("Registry server preflight failed, HTTP " + str(code))
        if code == 404:
            servers.append(plan)
        for item in plan["versions"]:
            version = item["server_json"].get("version")
            if not isinstance(version, str) or not re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?",version):
                raise ValueError("Invalid semver")
            if item["server_json"].get("name") != name or (name,version) in seen:
                raise ValueError("Mismatched/duplicate registry version")
            seen.add((name,version))
            if item["status"] not in {"draft", "active"}:
                raise ValueError("Only draft/active publication is supported; no deprecation/deletion is performed")
            if not all(isinstance(k,str) and isinstance(v,str) for k,v in item["metadata"].items()):
                raise ValueError("Metadata must be key/value strings")
            source = source_for(item, application)
            code, old = registry.request("GET",registry.path(name,version))
            if code == 404:
                action = "create"
            elif code == 200 and version_equal(old,item) and old.get("status") == item["status"]:
                action = "unchanged"
            elif code == 200 and old.get("source") == source and old.get("status") == "draft" and old.get("server_json") == item["server_json"] and tools_equal(old.get("tools"),item.get("tools")):
                # A retry may fill MISSING values on an owned draft only. A value
                # changed by another writer is a conflict, not silently repaired.
                if any(k in old.get("tags",{}) and old["tags"][k] != v for k,v in item["metadata"].items()):
                    raise RuntimeError("Owned draft metadata was changed externally: " + name + " " + version)
                action = "resume"
            elif code == 200:
                raise RuntimeError("Existing version conflicts with Git: " + name + " " + version + ". No writes performed; inspect drift or use a new version.")
            else:
                raise RuntimeError("Registry version preflight failed, HTTP " + str(code))
            actions.append((plan,item,source,action,old if code == 200 else None))

    def require(method,path,body=None):
        code,value = registry.request(method,path,body)
        if code not in {200,201,204}:
            raise RuntimeError("Registry " + method + " failed, HTTP " + str(code) + "; owned partial drafts may be resumed on the next sync")
        return value

    for plan in servers:
        require("POST","",{"name":plan["name"],"description":plan["description"]})
    result = []
    for plan,item,source,action,old in actions:
        name,version = plan["name"],item["server_json"]["version"]
        if action == "create":
            require("POST",registry.path(name)+"/versions",{"server_json":item["server_json"],
                "status":"draft", "tools":item.get("tools"), "source":source})
        if action != "unchanged":
            old_tags = (old or {}).get("tags",{})
            for key,value in item["metadata"].items():
                if old_tags.get(key) != value:
                    require("POST",registry.path(name,version)+"/tags",{"key":key,"value":value})
            if item["status"] == "active":
                require("PATCH",registry.path(name,version),{"status":"active"})
        got = require("GET",registry.path(name,version))
        if not version_equal(got,item) or got.get("status") != item["status"]:
            raise RuntimeError("Registry readback mismatch: " + name + " " + version)
        result.append({"name":name, "version":version, "status":got["status"], "action":action})
    return result


def ensure_access_endpoints(registry, plans, settings):
    """Create only missing native connection endpoints for the real active version."""
    if not enabled(settings,"ENSURE_ACCESS_ENDPOINT"):
        return []
    result=[]
    for plan in plans:
        if plan["name"] != settings["REGISTRY_SERVER_NAME"]:
            continue
        for item in plan["versions"]:
            if item["status"] != "active":
                raise ValueError("A usable access endpoint requires REAL_VERSION_STATUS=active")
            version=item["server_json"]["version"]
            endpoint_path=registry.path(plan["name"])+"/endpoints"
            query=urllib.parse.urlencode({"server_version":version,"max_results":100})
            code,body=registry.request("GET",endpoint_path+"?"+query)
            if code!=200:
                raise RuntimeError("Access endpoint lookup failed, HTTP "+str(code))
            found=list(body.get("mcp_access_endpoints",[]))
            cursor=body.get("next_page_token")
            while cursor:
                code,page=registry.request("GET",endpoint_path+"?"+query+"&"+urllib.parse.urlencode({"page_token":cursor}))
                if code!=200:
                    raise RuntimeError("Access endpoint pagination failed")
                found+=page.get("mcp_access_endpoints",[])
                cursor=page.get("next_page_token")
            remote=item["server_json"]["remotes"][0]
            matches=[e for e in found if e.get("url")==remote["url"] and e.get("transport_type")==remote["type"]]
            if matches:
                action="unchanged"
            else:
                code,created=registry.request("POST",endpoint_path,{"url":remote["url"],"transport_type":remote["type"],"server_version":version})
                if code not in {200,201}:
                    raise RuntimeError("Access endpoint creation failed, HTTP "+str(code))
                action="create"
            result.append({"name":plan["name"],"version":version,"action":action})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir",type=Path,default=Path("/data"))
    parser.add_argument("--apply",action="store_true",help="Execute registration; default is offline plan only")
    args = parser.parse_args()
    settings = dict(os.environ)
    if args.apply and settings.get("POD_NAMESPACE") != settings.get("REGISTRY_WORKSPACE"):
        raise ValueError("Run the Job in the target REGISTRY_WORKSPACE namespace; check the cluster overlay")
    if args.apply:
        resolve_route(settings)
    plans = build_plans(settings,args.data_dir,live=args.apply)
    if not args.apply:
        # Do not print metadata values or credentials. Discovery is deferred.
        print(json.dumps({"mode":"plan-only", "records":[{"name":p["name"], "versions":[v["server_json"]["version"] for v in p["versions"]]} for p in plans]},indent=2))
        return
    registry=Registry(settings)
    result = reconcile(registry,plans,settings["APPLICATION_ID"])
    endpoints=ensure_access_endpoints(registry,plans,settings)
    print(json.dumps({"workspace":settings["REGISTRY_WORKSPACE"], "versions":result,"access_endpoints":endpoints},indent=2))


if __name__ == "__main__":
    main()
