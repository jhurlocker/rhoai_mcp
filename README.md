# MCP registry registration with Argo CD

## jh-test-proj: project-local deployment and available canonical record

The `registry-registration/overlays/jh-test-proj` overlay now deploys its own
`MCPServer/rhoai-mcp`, Deployment, Service and authenticated HTTPS Route in
`jh-test-proj`. It registers **com.redhat/rhoai-mcp-server 1.29.1 as active** in
that workspace and creates its native access-endpoint entry. The separate
runtime in `rhoai-mcp` is not changed. This corrects the earlier
registration-only setup, which left drafts pointing at the other namespace.

The server uses OpenShift TokenReview and the caller's Kubernetes permissions.
Anonymous MCP requests are rejected; clients supply `Authorization: Bearer
<their OpenShift token>`. No real token is stored in Git. The Route hostname is
assigned by the target cluster, then read by the publisher, so no source-cluster
public hostname is embedded in this repository.

The full capability snapshot is captured from the project-local server as an
authorized administrator and committed as `registry/capabilities.json`. The
publisher verifies server version/protocol using its own limited identity;
it does not receive runtime workload permissions merely to enumerate every
administrator-visible tool. Tool invocation is still checked as the actual caller.

```bash
kustomize build registry-registration/overlays/jh-test-proj
oc apply -f registry-registration/argocd/jh-test-proj.yaml
oc get mcpserver,deploy,pod,service,route -n jh-test-proj
oc get application jh-test-proj-mcp-registration -n openshift-gitops
oc logs -n jh-test-proj job/mcp-registry-registration
```

The Application uses `main`. The canonical real server is the available
connection target. Earlier `*-poc` and synthetic `.invalid` records are retained
metadata-only history; the overlay no longer publishes those as deployed
servers (`REGISTER_POC=false`). A metadata fixture does not create an MCP runtime.
Use AI Hub's active/available view for the canonical server. Per-version history
is retained; the publisher performs no native deletion.

The general instructions below describe the registration-only **base/example**
overlay. Run their paths from `registry-registration/`. The `jh-test-proj`
overlay additionally contains `runtime/` and a reviewed capability snapshot;
it needs Argo authorization for the MCPServer, Route and uniquely named
TokenReview/SubjectAccessReview ClusterRole/Binding.

Copy **this directory's contents** into a new Git repository. It is a standalone
registration-only package for clusters that already have OpenShift AI and the MLflow-backed MCP registry installed and configured.
It has no imports, build paths, or runtime dependency on the original validation
repository. It requires no custom image build, `oc` binary, Python packages,
interactive login, or credentials committed to Git.

## What Argo deploys

```text
.
├── argocd/application.yaml       # Example Application: edit repo and destination
├── base/
│   ├── data/
│   │   ├── server.json           # Real server version/remote JSON template
│   │   ├── metadata.json         # Per-version owner/domain/contact/etc.
│   │   └── poc-versions.json     # Optional synthetic POC versions, JSON form
│   ├── runtime/                 # Standalone standard-library publisher/discovery
│   ├── settings.env             # Defaults, merged by the cluster overlay
│   ├── identity.yaml            # ServiceAccount + namespace-scoped registry role
│   ├── service-ca.yaml          # OpenShift-injected registry trust bundle
│   ├── job.yaml                 # Argo PostSync registration hook
│   └── kustomization.yaml
└── overlays/example/
    ├── kustomization.yaml       # Existing workspace namespace
    └── settings.env             # Cluster endpoints, version and metadata
```

The package creates a publisher ServiceAccount, Role/Binding, ConfigMaps and a
short-lived Job. It does not install/reconfigure the MCP server, MLflow, gateway,
operators, public routes, namespaces or the rest of the validation project.
The publisher has `get`, `list`, `create`, `update` on the MLflow
`mlflow.kubeflow.org/mcpservers` pseudo-resource in **one existing workspace**.
That grants access to MCP registry records in that workspace, not to runtime
tool invocation or workload administration. It has no registry-delete grant.

MLflow uses the pod's projected ServiceAccount token and the selected namespace
as workspace. The default internal endpoint was verified on the source cluster:
`https://mlflow.redhat-ods-applications.svc:8443/mlflow`. Edit it for your cluster.
Registry HTTPS uses the injected service CA; tokens are read from the projected
file on each request and are never logged. Neither registry nor MCP requests
follow redirects with credentials. See the official
[OpenShift AI MLflow authorization model](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/3.5/html/working_with_mlflow/installing-mlflow_mlflow).

## 1. Create your small repository

From the original project, copy only this folder:

```bash
cp -a gitops/registry-registration/. /path/to/your/new-repo/
cd /path/to/your/new-repo
git init
```

Do not copy the original `docs/`, `evidence/`, `tests/`, server-deployment YAML,
or repository-level scripts. This README and the files shown above are enough.
If you keep this package inside a larger repository instead, adjust the example
Application's `spec.source.path` to its overlay's full path.

## 2. Configure each cluster

Edit `overlays/example/kustomization.yaml` and `overlays/example/settings.env`.
Rename/copy that overlay for each target cluster. The namespace already exists.

| Setting | What to configure |
|---|---|
| `namespace` in `kustomization.yaml` | Existing registry workspace/project where the Job and its Role/Binding run. |
| `REGISTRY_WORKSPACE` | The same namespace. The Job refuses a mismatch. |
| `REGISTRY_URL` | Existing MLflow HTTPS base, **including `/mlflow`** for the shown installation, without `/api/3.0/...`. |
| `MCP_URL`, `MCP_NAMESPACE`, `MCP_NAME` | Existing MCP endpoint and deployment identity. Default is the internal service; no port-forward is needed in the cluster. |
| `SERVER_VERSION` | Actual running server semver. Discovery fails before publication if this does not match. |
| `REGISTRY_SERVER_NAME` | Logical registry server name. The default ends `-poc` to avoid overwriting an already registered real server. |
| `MCP_OWNER`, `MCP_SECURITY_DOMAIN`, `MCP_SUPPORT_CONTACT` | Your real metadata values; the Job rejects unchanged `replace-with-*` placeholders. |
| `REAL_VERSION_STATUS` | Default `draft`. Set `active` in Git only when the POC review permits activation. |
| `REGISTER_REAL` | Default `true`; set false for a synthetic-data-only Application. |
| `REGISTER_POC` | Default `false`; set true to also load the three synthetic draft versions. |
| `POC_RECORD_NAME` | Dedicated `io.example/mcp-acceptance-*` name for those synthetic records. |
| `APPLICATION_ID` | Stable ownership identity for resumable drafts, default `rhoai-mcp-registration`. Keep it stable across normal syncs. |

`base/data/server.json` and `base/data/metadata.json` are parsed templates,
substituted from settings at runtime. Add reviewed metadata there as needed.
Do not put tokens, credentials or protected policy bodies in these files.

By default the Job discovers current tools/resources/resource templates/prompts
from the real existing server, records full metadata, and verifies its version.
It never invokes a tool, reads resource content, or gets prompt content. It uses
the ServiceAccount token for discovery. If the server requires another credential,
set `MCP_AUTH_MODE=header` and inject `MCP_AUTHORIZATION` from an existing Secret
with a Job patch; keep the Secret out of Git. If metadata is intentionally public,
`MCP_AUTH_MODE=none` explicitly permits anonymous discovery. `DISCOVER_REAL=false`
skips discovery and stores tools as null/unknown, not a fabricated snapshot.

For a private HTTPS MCP endpoint set `MCP_CA_FILE` to a mounted trusted CA. The
registry's `REGISTRY_CA_FILE` defaults to the OpenShift service CA. External
registry certificates may require a separately mounted corporate/public trust
bundle. Certificate verification remains enabled. Existing network policies
must permit the Job's DNS, registry and metadata-discovery paths; this package
does not alter them. It installs no runtime access entitlements.

The default runner image is the public UBI Python 3.11 image used for the earlier
POC fixture. Set a mirrored approved digest with Kustomize `images` as shown in
the overlay before promoting this beyond the POC. Runtime needs only Python's
standard library; no image build or network package installation is necessary.

Use one registration Application per workspace. If multiple Applications must
share a workspace, give them unique Kustomize `namePrefix` values and stable,
distinct `APPLICATION_ID`s, and avoid managing the same server/version.

## 3. Preview and point Argo CD at the repository

```bash
kustomize build overlays/example > /tmp/mcp-registry-registration.yaml
```

Inspect the output. It should contain only the publisher identity/RBAC,
ConfigMaps and registration Job. There is no MCPServer, Deployment, Service,
HTTPRoute, Namespace or native registry CRD in it.

Edit `argocd/application.yaml`:

- Set `repoURL`, revision, overlay path, Argo project and Argo namespace.
- Set the destination cluster (`server` or configured cluster `name`) and the
  same existing workspace namespace as the overlay.
- Ensure that Argo's project/controller is already authorized to manage these
  namespace-scoped resources and grant the shown pseudo-resource Role/Binding.
  Platform admins can pre-provision the publisher identity if required; do not
  grant the hook cluster-admin as a workaround.

Then commit/push your repository and create the Application in the Argo control
cluster:

```bash
git add .
git commit -m "Manage MCP registry registration with Argo CD"
# Configure your remote, push, and let Argo access the repo normally.
oc apply -f argocd/application.yaml
```

For additional clusters create an overlay and Application per cluster, or let
your existing ApplicationSet generate them. Each Job runs on its Application's
destination cluster and calls that cluster's configured registry/server. The
Argo control cluster does not need direct connectivity to the MCP service.

The normal resources use sync wave `-1`. The Job is a `PostSync` hook, so it
runs after normal resources are applied and healthy. `BeforeHookCreation`
recreates the named Job on the next full sync; completed/failed Jobs are kept
for log inspection until then. Argo's
[hook](https://argo-cd.readthedocs.io/en/stable/user-guide/resource_hooks/) and
[sync-wave](https://argo-cd.readthedocs.io/en/stable/user-guide/sync-waves/)
documentation describes these phases. Hooks do not run in selective/resource-only
syncs; use a full Application sync. Do not add a TTL that independently removes
the Job if you want its logs retained.

The Application ignores only the injected CA ConfigMap's `/data` field and
uses `RespectIgnoreDifferences=true`, so Argo does not erase the OpenShift
controller's trust bundle. Keep that rule when adapting the Application; if
you add a Kustomize `namePrefix`, update the rule's ConfigMap name too. Other
input ConfigMaps remain fully managed by Git.

## 4. Applying the POC versions

Set `REGISTER_POC=true` in the overlay and sync. The Job reads
`base/data/poc-versions.json`, a dependency-free JSON representation of the
original `poc-versions.yaml` scenario. It preserves the same three versions,
synthetic tools and per-version metadata. Edit that file to change the POC data.
It is application data, not a Kubernetes manifest to apply directly.

The synthetic record is separate from the real server. Its `.invalid` remote
is never contacted. With the supplied data all three versions stay drafts;
there are no deprecation/deletion tests or echo tools published as the real
server's capability snapshot. Set `REGISTER_REAL=false` too if only the POC
version-data load is wanted.

## 5. Sync, retries, drift and history

The publisher preflights **all** selected records/versions before writes:

- Missing versions are created as draft, metadata is stored, then requested
  activation is applied and read back.
- Exact matching versions are unchanged on repeat sync.
- On interruption, a later sync can fill missing metadata on **this
  Application's own matching draft**. Ownership is tied to a deterministic
  source/content hash, not just a record name.
- Git may promote its own complete matching draft to active. A manual/other
  application's draft is not silently promoted or adopted for writes.
- Different existing content, managed metadata, capabilities or incompatible
  lifecycle state causes sync failure. No older user-created version is rewritten.
  Use a new semver or resolve the conflicting record deliberately.
- No DELETE operation is implemented. Removing a version/file, disabling a data
  set, pruning Kubernetes resources, rolling Git back, or deleting this
  Application does not remove registry history or reverse earlier API writes.

Once published, treat a version's metadata/capability snapshot as immutable in
this POC workflow. Record changes as a new version. If runtime capabilities drift
without a version bump, the next full sync detects the changed snapshot and
fails rather than silently overwriting history. Reverting Git does not undo a
publication or automatically reactivate a deprecated version. Native-deleted
semvers remain reserved; use a new version or dedicated POC record name.

This is GitOps **through an API Job**, not a native controller for registry data:
Argo tracks the Kubernetes inputs and Job result, but does not continuously
compare the external registry database. Resync/retry reruns the checks; a manual
registry edit is not automatically shown as Argo OutOfSync until checked. Partial
logical records/drafts can remain after an API/network failure; inspect them and
retry. Do not use selective sync to repair registry data.

## 6. Verify

```bash
oc logs -n rhoai-mcp job/mcp-registry-registration
oc get job -n rhoai-mcp mcp-registry-registration
```

Use your overlay's namespace. Successful output lists only names, versions,
statuses and create/resume/unchanged actions, not tokens or metadata values.
Check the same workspace/server/versions and metadata in AI Hub. A failed hook
makes the sync fail; inspect logs, permissions, trust, connectivity, runtime
version and content conflicts before retrying a full sync.

Prepared/checked October 30, 2026. The standalone copy was built with Kustomize
and passed 12 offline publication/copy-portability tests. All generated resources
and the example Argo Application passed server-side dry-run. Internal
HTTPS/service-account registry access was verified using an existing MLflow pod.
The initial preparation did not install an Application. The subsequent
`jh-test-proj` live rollout and repeat-sync results are recorded above.
