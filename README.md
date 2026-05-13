# gpu-reaper

Automatically detects and scales down idle GPU workloads on shared Kubernetes/OpenShift clusters. Runs as a CronJob, queries the cluster's DCGM exporter for per-pod GPU utilization, and scales down Deployments or deletes standalone pods that have been idle beyond a configurable threshold.

## How it works

Every 5 minutes, the CronJob:

1. Lists all Running pods with `nvidia.com/gpu` resource requests
2. For each pod, checks idle status:
   - **Primary**: Queries the node-local DCGM exporter for `DCGM_FI_DEV_GPU_UTIL` filtered by pod — works for any GPU workload (vLLM, sglang, training, debug pods) without exec
   - **Fallback 1**: Scrapes vLLM `/metrics` (`vllm:num_requests_running`, `vllm:num_requests_waiting`)
   - **Fallback 2**: Execs `nvidia-smi` in the GPU container
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

| Variable                   | Default                              | Description                                                                |
|----------------------------|--------------------------------------|----------------------------------------------------------------------------|
| `IDLE_THRESHOLD_MINUTES`   | `120`                                | Minutes of inactivity before scaling down                                  |
| `DRY_RUN`                  | `true`                               | Set to `false` to actually scale down workloads                            |
| `NAMESPACE`                | _(all)_                              | Scope to a single namespace                                                |
| `EXCLUDE_NAMESPACES`       | `kube-system,openshift-*,gpu-reaper` | Comma-separated, supports glob patterns                                    |
| `VLLM_METRICS_PORT`        | `8000`                               | Port where vLLM serves `/metrics`                                          |
| `GPU_UTIL_THRESHOLD`       | `5`                                  | GPU utilization % below which a GPU is considered idle (DCGM and nvidia-smi) |
| `DCGM_EXPORTER_NAMESPACE`  | `nvidia-gpu-operator`                | Namespace where the DCGM exporter DaemonSet runs                           |
| `DCGM_EXPORTER_LABEL`      | `app=nvidia-dcgm-exporter`           | Label selector for DCGM exporter pods                                      |
| `DCGM_EXPORTER_PORT`       | `9400`                               | Port serving DCGM `/metrics`                                               |

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
        │     ├── GET http://{dcgm_exporter_ip}:9400/metrics  (DCGM, primary)
        │     ├── GET http://{pod_ip}:8000/metrics            (vLLM fallback)
        │     └── exec nvidia-smi                             (last resort)
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

- **Polling-based**: Checks every 5 minutes. A deployment that becomes active between checks won't be detected until the next run. The idle threshold (2h default) makes false positives unlikely.
- **DCGM exporter required for full coverage**: Without it, the reaper falls back to vLLM `/metrics` and `nvidia-smi` exec — both miss many workloads (non-vLLM pods, containers without `nvidia-smi`, exec-restricted clusters). Set `DCGM_EXPORTER_*` to match your install (default targets the NVIDIA GPU Operator).
- **CrashLoopBackOff pods**: GPU pods stuck in crash loops still hold GPU allocations but can't be measured. The reaper conservatively skips them.
- **nvidia-smi exec**: Requires the target container to have `nvidia-smi` available and the pod's SecurityContext to allow exec. Most CUDA-based images include it.
- **ConfigMap size**: State is stored in a ConfigMap (~200 bytes per tracked pod). Supports ~5000 concurrent GPU pods before hitting the 1 MiB limit.
