#!/usr/bin/env python3
"""gpu-reaper: Detect and scale down idle GPU workloads on shared Kubernetes/OpenShift clusters."""

import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch
from typing import Optional

import requests as http_requests
from kubernetes import client, config
from kubernetes.stream import stream

log = logging.getLogger("gpu-reaper")

ANNOTATION_EXCLUDE = "gpu-reaper.io/exclude"
ANNOTATION_SCALED_DOWN_AT = "gpu-reaper.io/scaled-down-at"
ANNOTATION_REASON = "gpu-reaper.io/reason"
STATE_CONFIGMAP_NAME = "gpu-reaper-state"
GPU_RESOURCE = "nvidia.com/gpu"


# ── Configuration ──────────────────────────────────────────────────────────


@dataclass
class ReaperConfig:
    idle_threshold: timedelta
    dry_run: bool
    namespace: Optional[str]
    exclude_namespaces: list[str]
    vllm_metrics_port: int
    gpu_util_threshold: int
    reaper_namespace: str
    dcgm_namespace: str
    dcgm_label_selector: str
    dcgm_port: int


def load_config() -> ReaperConfig:
    return ReaperConfig(
        idle_threshold=timedelta(minutes=int(os.getenv("IDLE_THRESHOLD_MINUTES", "120"))),
        dry_run=os.getenv("DRY_RUN", "true").lower() != "false",
        namespace=os.getenv("NAMESPACE") or None,
        exclude_namespaces=[
            s.strip()
            for s in os.getenv("EXCLUDE_NAMESPACES", "kube-system,openshift-*,gpu-reaper").split(",")
            if s.strip()
        ],
        vllm_metrics_port=int(os.getenv("VLLM_METRICS_PORT", "8000")),
        gpu_util_threshold=int(os.getenv("GPU_UTIL_THRESHOLD", "5")),
        reaper_namespace=os.getenv("REAPER_NAMESPACE", "gpu-reaper"),
        dcgm_namespace=os.getenv("DCGM_EXPORTER_NAMESPACE", "nvidia-gpu-operator"),
        dcgm_label_selector=os.getenv("DCGM_EXPORTER_LABEL", "app=nvidia-dcgm-exporter"),
        dcgm_port=int(os.getenv("DCGM_EXPORTER_PORT", "9400")),
    )


# ── Kubernetes helpers ─────────────────────────────────────────────────────


def load_kube_clients() -> tuple[client.CoreV1Api, client.AppsV1Api]:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    return client.CoreV1Api(), client.AppsV1Api()


def namespace_excluded(ns: str, patterns: list[str]) -> bool:
    return any(fnmatch(ns, p) for p in patterns)


def pod_has_gpu(pod: client.V1Pod) -> bool:
    for c in pod.spec.containers:
        reqs = (c.resources.requests or {}) if c.resources else {}
        limits = (c.resources.limits or {}) if c.resources else {}
        if GPU_RESOURCE in reqs or GPU_RESOURCE in limits:
            return True
    return False


def gpu_count(pod: client.V1Pod) -> int:
    total = 0
    for c in pod.spec.containers:
        limits = (c.resources.limits or {}) if c.resources else {}
        total += int(limits.get(GPU_RESOURCE, 0))
    return total


def gpu_container_name(pod: client.V1Pod) -> str:
    """Find the container that has the GPU resource request."""
    for c in pod.spec.containers:
        limits = (c.resources.limits or {}) if c.resources else {}
        if GPU_RESOURCE in limits:
            return c.name
    return pod.spec.containers[0].name


def get_excluded_namespaces_by_annotation(core_v1: client.CoreV1Api) -> set[str]:
    """Return set of namespace names that have the gpu-reaper.io/exclude annotation."""
    excluded = set()
    try:
        namespaces = core_v1.list_namespace().items
        for ns_obj in namespaces:
            annotations = ns_obj.metadata.annotations or {}
            if annotations.get(ANNOTATION_EXCLUDE, "").lower() == "true":
                excluded.add(ns_obj.metadata.name)
    except client.ApiException as e:
        log.warning("Failed to list namespaces for annotation check: %s", e.reason)
    return excluded


def get_gpu_pods(core_v1: client.CoreV1Api, cfg: ReaperConfig) -> list[client.V1Pod]:
    if cfg.namespace:
        pods = core_v1.list_namespaced_pod(cfg.namespace, field_selector="status.phase=Running").items
    else:
        pods = core_v1.list_pod_for_all_namespaces(field_selector="status.phase=Running").items

    excluded_ns_annotations = get_excluded_namespaces_by_annotation(core_v1)

    result = []
    for pod in pods:
        ns = pod.metadata.namespace
        if namespace_excluded(ns, cfg.exclude_namespaces):
            continue
        if ns in excluded_ns_annotations:
            log.info("[SKIP] %s/%s -- namespace has %s annotation", ns, pod.metadata.name, ANNOTATION_EXCLUDE)
            continue
        annotations = pod.metadata.annotations or {}
        if annotations.get(ANNOTATION_EXCLUDE, "").lower() == "true":
            log.info("[SKIP] %s/%s -- has %s annotation", ns, pod.metadata.name, ANNOTATION_EXCLUDE)
            continue
        if pod_has_gpu(pod):
            result.append(pod)
    return result


# ── Owner resolution ───────────────────────────────────────────────────────


def resolve_owner(
    pod: client.V1Pod,
    apps_v1: client.AppsV1Api,
) -> tuple[str, str, str]:
    """Walk ownerReferences to find the scalable parent.

    Returns (kind, name, namespace) where kind is one of:
    Deployment, ReplicaSet, StatefulSet, Pod (standalone), Skip.
    """
    ns = pod.metadata.namespace
    owners = pod.metadata.owner_references or []

    if not owners:
        return "Pod", pod.metadata.name, ns

    owner = owners[0]

    if owner.kind == "ReplicaSet":
        try:
            rs = apps_v1.read_namespaced_replica_set(owner.name, ns)
            rs_owners = rs.metadata.owner_references or []
            if rs_owners and rs_owners[0].kind == "Deployment":
                # Check if the deployment also has the exclude annotation
                dep = apps_v1.read_namespaced_deployment(rs_owners[0].name, ns)
                dep_annotations = dep.metadata.annotations or {}
                if dep_annotations.get(ANNOTATION_EXCLUDE, "").lower() == "true":
                    log.info(
                        "[SKIP] %s/%s -- parent Deployment %s has %s annotation",
                        ns, pod.metadata.name, rs_owners[0].name, ANNOTATION_EXCLUDE,
                    )
                    return "Skip", rs_owners[0].name, ns
                return "Deployment", rs_owners[0].name, ns
        except client.ApiException:
            pass
        return "ReplicaSet", owner.name, ns

    if owner.kind == "StatefulSet":
        return "StatefulSet", owner.name, ns

    # Job, DaemonSet, etc. — skip
    return "Skip", owner.name, ns


# ── Idle detection ─────────────────────────────────────────────────────────


def parse_prometheus_gauge(text: str, metric_name: str) -> Optional[float]:
    """Sum all samples for a given Prometheus metric from text exposition format."""
    total = 0.0
    found = False
    pattern = re.compile(
        rf"^{re.escape(metric_name)}(?:\{{[^}}]*\}})?\s+(\S+)$", re.MULTILINE
    )
    for match in pattern.finditer(text):
        found = True
        try:
            total += float(match.group(1))
        except ValueError:
            continue
    return total if found else None


def check_vllm_metrics(pod_ip: str, port: int) -> Optional[bool]:
    """Hit vLLM /metrics endpoint. Returns True if idle, False if active, None if unreachable."""
    try:
        resp = http_requests.get(f"http://{pod_ip}:{port}/metrics", timeout=5)
        resp.raise_for_status()
        text = resp.text
    except Exception:
        return None

    running = parse_prometheus_gauge(text, "vllm:num_requests_running")
    waiting = parse_prometheus_gauge(text, "vllm:num_requests_waiting")

    if running is None and waiting is None:
        return None  # Not a vLLM endpoint

    r = running if running is not None else 0.0
    w = waiting if waiting is not None else 0.0
    return r == 0.0 and w == 0.0


_DCGM_EXPORTER_CACHE: dict[str, Optional[str]] = {}


def find_dcgm_exporter_ip(
    core_v1: client.CoreV1Api,
    node_name: str,
    dcgm_ns: str,
    dcgm_label: str,
) -> Optional[str]:
    """Locate the DCGM exporter pod IP on a given node. Cached per-run."""
    if node_name in _DCGM_EXPORTER_CACHE:
        return _DCGM_EXPORTER_CACHE[node_name]
    try:
        exporters = core_v1.list_namespaced_pod(
            dcgm_ns,
            label_selector=dcgm_label,
            field_selector=f"spec.nodeName={node_name},status.phase=Running",
        ).items
    except client.ApiException:
        _DCGM_EXPORTER_CACHE[node_name] = None
        return None
    ip = exporters[0].status.pod_ip if exporters else None
    _DCGM_EXPORTER_CACHE[node_name] = ip
    return ip


def check_dcgm_metrics(
    core_v1: client.CoreV1Api,
    pod: client.V1Pod,
    cfg: "ReaperConfig",
) -> Optional[bool]:
    """Query the node-local DCGM exporter for this pod's GPU utilization.

    Returns True if every GPU assigned to the pod is below threshold,
    False if any GPU is above, None if metrics are unavailable.
    """
    node_name = pod.spec.node_name
    if not node_name:
        return None

    exporter_ip = find_dcgm_exporter_ip(core_v1, node_name, cfg.dcgm_namespace, cfg.dcgm_label_selector)
    if not exporter_ip:
        return None

    try:
        resp = http_requests.get(
            f"http://{exporter_ip}:{cfg.dcgm_port}/metrics", timeout=5,
        )
        resp.raise_for_status()
        text = resp.text
    except Exception:
        return None

    pod_name = pod.metadata.name
    pod_ns = pod.metadata.namespace
    utils: list[float] = []
    for line in text.split("\n"):
        if not line.startswith("DCGM_FI_DEV_GPU_UTIL{"):
            continue
        if f'pod="{pod_name}"' not in line:
            continue
        if f'namespace="{pod_ns}"' not in line:
            continue
        m = re.search(r"\}\s+(\S+)\s*$", line)
        if not m:
            continue
        try:
            utils.append(float(m.group(1)))
        except ValueError:
            continue
    if not utils:
        return None
    return max(utils) < cfg.gpu_util_threshold


def check_nvidia_smi(
    core_v1: client.CoreV1Api,
    pod: client.V1Pod,
    threshold: int,
) -> Optional[bool]:
    """Exec nvidia-smi in the GPU container. Returns True if idle, False if active, None on failure."""
    container = gpu_container_name(pod)
    try:
        resp = stream(
            core_v1.connect_get_namespaced_pod_exec,
            pod.metadata.name,
            pod.metadata.namespace,
            container=container,
            command=[
                "nvidia-smi",
                "--query-gpu=utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            stderr=True,
            stdout=True,
            stdin=False,
            tty=False,
        )
        lines = [line.strip() for line in resp.strip().split("\n") if line.strip()]
        utils = [int(line) for line in lines]
        return all(u < threshold for u in utils)
    except Exception:
        return None


def is_pod_idle(
    core_v1: client.CoreV1Api,
    pod: client.V1Pod,
    cfg: ReaperConfig,
) -> Optional[bool]:
    """Returns True if idle, False if active, None if unable to determine.

    Order: DCGM exporter (cluster-wide GPU util, no exec) -> vLLM metrics ->
    in-pod nvidia-smi exec. DCGM is primary because it works for every GPU
    workload (vLLM, sglang, training, debug pods) without requiring a working
    /metrics endpoint or exec permission.
    """
    dcgm_result = check_dcgm_metrics(core_v1, pod, cfg)
    if dcgm_result is not None:
        return dcgm_result

    pod_ip = pod.status.pod_ip
    if pod_ip:
        vllm_result = check_vllm_metrics(pod_ip, cfg.vllm_metrics_port)
        if vllm_result is not None:
            log.info(
                "[FALLBACK] %s/%s -- DCGM unavailable, vLLM /metrics: %s",
                pod.metadata.namespace,
                pod.metadata.name,
                "idle" if vllm_result else "active",
            )
            return vllm_result

    smi_result = check_nvidia_smi(core_v1, pod, cfg.gpu_util_threshold)
    if smi_result is not None:
        log.info(
            "[FALLBACK] %s/%s -- DCGM/vLLM unavailable, nvidia-smi: %s",
            pod.metadata.namespace,
            pod.metadata.name,
            "idle" if smi_result else "active",
        )
        return smi_result

    return None


# ── State management ───────────────────────────────────────────────────────


def load_state(core_v1: client.CoreV1Api, namespace: str) -> tuple[dict, str]:
    """Load state from ConfigMap. Returns (state_dict, resource_version)."""
    try:
        cm = core_v1.read_namespaced_config_map(STATE_CONFIGMAP_NAME, namespace)
        data = json.loads((cm.data or {}).get("state", "{}"))
        return data, cm.metadata.resource_version
    except client.ApiException as e:
        if e.status == 404:
            # Create the ConfigMap
            cm = client.V1ConfigMap(
                metadata=client.V1ObjectMeta(name=STATE_CONFIGMAP_NAME, namespace=namespace),
                data={"state": "{}"},
            )
            created = core_v1.create_namespaced_config_map(namespace, cm)
            return {}, created.metadata.resource_version
        raise


def save_state(core_v1: client.CoreV1Api, namespace: str, state: dict, resource_version: str):
    body = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=STATE_CONFIGMAP_NAME,
            namespace=namespace,
            resource_version=resource_version,
        ),
        data={"state": json.dumps(state)},
    )
    core_v1.replace_namespaced_config_map(STATE_CONFIGMAP_NAME, namespace, body)


def cleanup_state(state: dict, live_pod_uids: set[str]) -> dict:
    stale = [uid for uid in state if uid not in live_pod_uids]
    for uid in stale:
        log.info("[CLEANUP] Removing stale entry for pod %s (%s/%s)",
                 uid, state[uid].get("namespace", "?"), state[uid].get("pod_name", "?"))
    return {uid: v for uid, v in state.items() if uid in live_pod_uids}


# ── Actions ────────────────────────────────────────────────────────────────


def annotate_resource(api, kind: str, name: str, namespace: str, annotations: dict[str, str]):
    body = {"metadata": {"annotations": annotations}}
    try:
        if kind == "Deployment":
            api.patch_namespaced_deployment(name, namespace, body)
        elif kind == "ReplicaSet":
            api.patch_namespaced_replica_set(name, namespace, body)
        elif kind == "StatefulSet":
            api.patch_namespaced_stateful_set(name, namespace, body)
    except client.ApiException as e:
        log.warning("Failed to annotate %s %s/%s: %s", kind, namespace, name, e.reason)


def annotate_pod(core_v1: client.CoreV1Api, pod: client.V1Pod, annotations: dict[str, str]):
    body = {"metadata": {"annotations": annotations}}
    try:
        core_v1.patch_namespaced_pod(pod.metadata.name, pod.metadata.namespace, body)
    except client.ApiException as e:
        log.warning("Failed to annotate pod %s/%s: %s", pod.metadata.namespace, pod.metadata.name, e.reason)


def scale_down(apps_v1: client.AppsV1Api, kind: str, name: str, namespace: str, dry_run: bool):
    body = {"spec": {"replicas": 0}}
    prefix = "[DRY RUN]" if dry_run else "[ACTION]"
    log.info("%s Scaling %s %s/%s to 0 replicas", prefix, kind, namespace, name)
    if dry_run:
        return
    try:
        if kind == "Deployment":
            apps_v1.patch_namespaced_deployment(name, namespace, body)
        elif kind == "ReplicaSet":
            apps_v1.patch_namespaced_replica_set(name, namespace, body)
        elif kind == "StatefulSet":
            apps_v1.patch_namespaced_stateful_set(name, namespace, body)
    except client.ApiException as e:
        log.error("Failed to scale %s %s/%s: %s", kind, namespace, name, e.reason)


def delete_pod(core_v1: client.CoreV1Api, pod: client.V1Pod, dry_run: bool):
    ns = pod.metadata.namespace
    name = pod.metadata.name
    prefix = "[DRY RUN]" if dry_run else "[ACTION]"
    log.info("%s Deleting standalone pod %s/%s", prefix, ns, name)
    if dry_run:
        return
    try:
        core_v1.delete_namespaced_pod(name, ns)
    except client.ApiException as e:
        log.error("Failed to delete pod %s/%s: %s", ns, name, e.reason)


def take_action(
    core_v1: client.CoreV1Api,
    apps_v1: client.AppsV1Api,
    pod: client.V1Pod,
    owner_kind: str,
    owner_name: str,
    idle_since: str,
    cfg: ReaperConfig,
):
    now_str = datetime.now(timezone.utc).isoformat()
    gpus = gpu_count(pod)
    ns = pod.metadata.namespace
    reason = f"Idle since {idle_since} ({cfg.idle_threshold.total_seconds() / 60:.0f}m threshold exceeded), freeing {gpus} GPU(s)"

    annotations = {
        ANNOTATION_SCALED_DOWN_AT: now_str,
        ANNOTATION_REASON: reason,
    }

    if owner_kind == "Pod":
        if not cfg.dry_run:
            annotate_pod(core_v1, pod, annotations)
        delete_pod(core_v1, pod, cfg.dry_run)
    else:
        if not cfg.dry_run:
            annotate_resource(apps_v1, owner_kind, owner_name, ns, annotations)
        scale_down(apps_v1, owner_kind, owner_name, ns, cfg.dry_run)


# ── Main ───────────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        stream=sys.stdout,
    )
    logging.Formatter.converter = lambda *_: datetime.now(timezone.utc).timetuple()

    cfg = load_config()
    log.info(
        "gpu-reaper starting -- dry_run=%s, threshold=%dm, namespace=%s",
        cfg.dry_run,
        cfg.idle_threshold.total_seconds() / 60,
        cfg.namespace or "(all)",
    )

    core_v1, apps_v1 = load_kube_clients()
    pods = get_gpu_pods(core_v1, cfg)
    log.info("Found %d GPU pods", len(pods))

    state, rv = load_state(core_v1, cfg.reaper_namespace)
    now = datetime.now(timezone.utc)
    live_uids: set[str] = set()
    actions_taken = 0
    idle_count = 0

    # Track which owners we already acted on to avoid duplicate actions
    # (multiple pods can belong to the same Deployment)
    acted_owners: set[str] = set()

    for pod in pods:
        uid = pod.metadata.uid
        ns = pod.metadata.namespace
        name = pod.metadata.name
        live_uids.add(uid)

        owner_kind, owner_name, owner_ns = resolve_owner(pod, apps_v1)
        if owner_kind == "Skip":
            log.info("[SKIP] %s/%s -- owned by %s (not a scalable workload)", ns, name, owner_name)
            continue

        owner_key = f"{owner_kind}/{owner_ns}/{owner_name}"
        if owner_key in acted_owners:
            continue

        idle = is_pod_idle(core_v1, pod, cfg)

        if idle is None:
            log.info("[UNKNOWN] %s/%s -- unable to determine idle status, skipping", ns, name)
            if uid in state:
                del state[uid]
            continue

        if idle:
            idle_count += 1
            if uid not in state:
                state[uid] = {
                    "first_idle": now.isoformat(),
                    "namespace": ns,
                    "pod_name": name,
                    "owner_kind": owner_kind,
                    "owner_name": owner_name,
                    "gpus": gpu_count(pod),
                }
                log.info("[NEW IDLE] %s/%s -- tracking started (owner: %s/%s, GPUs: %d)",
                         ns, name, owner_kind, owner_name, gpu_count(pod))
            else:
                idle_since = datetime.fromisoformat(state[uid]["first_idle"])
                idle_duration = now - idle_since
                if idle_duration >= cfg.idle_threshold:
                    log.info(
                        "[THRESHOLD] %s/%s -- idle for %s (threshold: %s), owner: %s/%s, GPUs: %d",
                        ns, name, idle_duration, cfg.idle_threshold,
                        owner_kind, owner_name, gpu_count(pod),
                    )
                    take_action(core_v1, apps_v1, pod, owner_kind, owner_name,
                                state[uid]["first_idle"], cfg)
                    actions_taken += 1
                    acted_owners.add(owner_key)
                    del state[uid]
                else:
                    remaining = cfg.idle_threshold - idle_duration
                    log.info("[IDLE] %s/%s -- idle %s, %s until action",
                             ns, name, idle_duration, remaining)
        else:
            if uid in state:
                log.info("[ACTIVE] %s/%s -- removed from idle tracking", ns, name)
                del state[uid]
            else:
                log.info("[ACTIVE] %s/%s", ns, name)

    state = cleanup_state(state, live_uids)
    save_state(core_v1, cfg.reaper_namespace, state, rv)

    log.info(
        "Summary: %d pods scanned, %d idle, %d actions%s",
        len(pods), idle_count, actions_taken,
        " (dry run)" if cfg.dry_run else "",
    )


if __name__ == "__main__":
    main()
