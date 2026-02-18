# gpu-reaper

Automatically detects and scales down idle GPU workloads on shared Kubernetes/OpenShift clusters. Runs as a CronJob, scrapes vLLM metrics to detect idle inference engines, and scales down Deployments or deletes standalone pods that have been idle beyond a configurable threshold.

## How it works

Every 5 minutes, the CronJob:

1. Lists all Running pods with `nvidia.com/gpu` resource requests
2. For each pod, checks idle status:
   - **Primary**: Scrapes vLLM `/metrics` endpoint — checks `vllm:num_requests_running` and `vllm:num_requests_waiting`
   - **Fallback**: Execs `nvidia-smi` to check GPU compute utilization (for non-vLLM workloads)
3. Tracks idle duration in a ConfigMap
4. When idle time exceeds the threshold:
   - **Deployments** → scaled to 0 replicas
   - **Standalone pods** → deleted
   - Job/DaemonSet-owned pods → skipped

## Quick start

```bash
# Build the image
docker build --platform linux/amd64 -t <your-registry>/gpu-reaper:latest .
docker push <your-registry>/gpu-reaper:latest

# Update the image reference in the CronJob manifest
sed -i 's|gpu-reaper:latest|<your-registry>/gpu-reaper:latest|' manifests/cronjob.yaml

# Deploy (starts in dry-run mode)
kubectl apply -f manifests/
```

Check the logs to see what it would do:

```bash
kubectl -n gpu-reaper logs -l app.kubernetes.io/name=gpu-reaper --tail=100
```

## Go live

Once you're satisfied with the dry-run output:

```bash
kubectl -n gpu-reaper set env cronjob/gpu-reaper DRY_RUN=false
```

## Configuration

All settings are environment variables on the CronJob container:

| Variable | Default | Description |
|----------|---------|-------------|
| `IDLE_THRESHOLD_MINUTES` | `30` | Minutes of inactivity before scaling down |
| `DRY_RUN` | `true` | Set to `false` to actually scale down workloads |
| `NAMESPACE` | _(all)_ | Scope to a single namespace |
| `EXCLUDE_NAMESPACES` | `kube-system,openshift-*,gpu-reaper` | Comma-separated, supports glob patterns |
| `VLLM_METRICS_PORT` | `8000` | Port where vLLM serves `/metrics` |
| `GPU_UTIL_THRESHOLD` | `5` | GPU utilization % below which nvidia-smi considers a GPU idle |

## Opt out a workload

Add the `gpu-reaper.io/exclude=true` annotation at any level — namespace, deployment, or pod:

```bash
# Exclude an entire namespace
kubectl annotate namespace/team-prod gpu-reaper.io/exclude=true

# Exclude a specific deployment
kubectl annotate deployment/my-vllm gpu-reaper.io/exclude=true

# Exclude a specific pod
kubectl annotate pod/my-pod gpu-reaper.io/exclude=true
```

Namespace-level exclusion skips all GPU pods in that namespace, regardless of per-pod or per-deployment annotations.

## Restore a scaled-down deployment

```bash
kubectl scale deployment/<name> --replicas=<original-count> -n <namespace>
```

The reaper annotates targets before acting — check the annotations for context:

```bash
kubectl get deployment <name> -n <namespace> -o jsonpath='{.metadata.annotations}'
```

## Architecture

```
CronJob (every 5 min)
  └── reaper.py
        ├── List GPU pods (all namespaces)
        ├── For each pod:
        │     ├── GET http://{pod_ip}:8000/metrics  (vLLM)
        │     └── exec nvidia-smi                   (fallback)
        ├── Track idle state in ConfigMap
        └── Scale down / delete if threshold exceeded
```

## RBAC

The ClusterRole grants:
- `namespaces`: get, list (for namespace-level exclusion annotations)
- `pods`: get, list, patch, delete (patch for annotations, delete for standalone pods)
- `pods/exec`: create (for nvidia-smi fallback)
- `deployments`, `replicasets`, `statefulsets`: get, list, patch (for scale-down)
- `configmaps`: get, create, update, patch (for idle state tracking)

## Limitations

- **Polling-based**: Checks every 5 minutes. A deployment that becomes active between checks won't be detected until the next run. The idle threshold (30 min default) makes false positives unlikely.
- **CrashLoopBackOff pods**: GPU pods stuck in crash loops still hold GPU allocations but can't be measured. The reaper conservatively skips them.
- **nvidia-smi exec**: Requires the target container to have `nvidia-smi` available and the pod's SecurityContext to allow exec. Most CUDA-based images include it.
- **ConfigMap size**: State is stored in a ConfigMap (~200 bytes per tracked pod). Supports ~5000 concurrent GPU pods before hitting the 1 MiB limit.
