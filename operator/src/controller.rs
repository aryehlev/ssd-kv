//! Reconciliation controller for SsdkvCluster resources.

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::apps::v1::StatefulSet;
use k8s_openapi::api::core::v1::{ConfigMap, Service};
use kube::api::{Api, Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Client, CustomResource, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{error, info};

use crate::resources;
use crate::shard_manager;

/// The spec for an SsdkvCluster resource.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "ssdkv.io",
    version = "v1alpha1",
    kind = "SsdkvCluster",
    namespaced,
    status = "SsdkvClusterStatus"
)]
#[serde(rename_all = "camelCase")]
pub struct SsdkvClusterSpec {
    /// Number of nodes in the cluster.
    #[serde(default = "default_replicas")]
    pub replicas: i32,

    /// Replication factor.
    #[serde(default = "default_replication_factor")]
    pub replication_factor: i32,

    /// Storage configuration.
    #[serde(default)]
    pub storage: StorageSpec,

    /// Container image.
    #[serde(default = "default_image")]
    pub image: String,

    /// Redis-compatible port.
    #[serde(default = "default_redis_port")]
    pub redis_port: i32,

    /// Cluster communication port.
    #[serde(default = "default_cluster_port")]
    pub cluster_port: i32,

    /// Resource requests/limits.
    #[serde(default)]
    pub resources: Option<ResourceSpec>,

    /// Allow replica nodes to serve read requests (READONLY command).
    #[serde(default)]
    pub replica_read: bool,
}

fn default_replicas() -> i32 { 3 }
fn default_replication_factor() -> i32 { 2 }
fn default_image() -> String { "ssd-kv:latest".to_string() }
fn default_redis_port() -> i32 { 7777 }
fn default_cluster_port() -> i32 { 7780 }

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    #[serde(default = "default_storage_size")]
    pub size: String,
    pub storage_class: Option<String>,
}

fn default_storage_size() -> String { "10Gi".to_string() }

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ResourceSpec {
    pub requests: Option<ResourceValues>,
    pub limits: Option<ResourceValues>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ResourceValues {
    pub cpu: Option<String>,
    pub memory: Option<String>,
}

/// Status subresource for SsdkvCluster.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct SsdkvClusterStatus {
    pub phase: Option<String>,
    pub ready_replicas: Option<i32>,
    pub topology_version: Option<i64>,
}

/// Shared context for the controller.
pub struct Context {
    pub client: Client,
}

/// Main reconciliation function.
pub async fn reconcile(
    cluster: Arc<SsdkvCluster>,
    ctx: Arc<Context>,
) -> Result<Action, kube::Error> {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    info!("Reconciling SsdkvCluster {}/{}", namespace, name);

    let client = &ctx.client;

    // 1. Create or update headless Service
    let service = resources::build_headless_service(&cluster);
    let svc_api: Api<Service> = Api::namespaced(client.clone(), &namespace);
    svc_api
        .patch(
            &format!("{}-headless", name),
            &PatchParams::apply("ssdkv-operator"),
            &Patch::Apply(service),
        )
        .await?;
    info!("Headless service ensured for {}", name);

    // 2. Create or update client Service
    let client_service = resources::build_client_service(&cluster);
    svc_api
        .patch(
            &name,
            &PatchParams::apply("ssdkv-operator"),
            &Patch::Apply(client_service),
        )
        .await?;
    info!("Client service ensured for {}", name);

    // 2b. Create or update readonly Service when replica_read is enabled
    if cluster.spec.replica_read {
        let readonly_service = resources::build_readonly_service(&cluster);
        svc_api
            .patch(
                &format!("{}-readonly", name),
                &PatchParams::apply("ssdkv-operator"),
                &Patch::Apply(readonly_service),
            )
            .await?;
        info!("Readonly service ensured for {}", name);
    }

    // 3. Compute shard assignments
    let shard_map = shard_manager::compute_shard_map(
        cluster.spec.replicas as u32,
        cluster.spec.replication_factor as u32,
    );

    // 4. Create or update topology ConfigMap
    let config_map = resources::build_topology_configmap(&cluster, &shard_map);
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), &namespace);
    cm_api
        .patch(
            &format!("{}-topology", name),
            &PatchParams::apply("ssdkv-operator"),
            &Patch::Apply(config_map),
        )
        .await?;
    info!("Topology configmap ensured for {}", name);

    // 5. Create or update StatefulSet
    let statefulset = resources::build_statefulset(&cluster);
    let ss_api: Api<StatefulSet> = Api::namespaced(client.clone(), &namespace);
    ss_api
        .patch(
            &name,
            &PatchParams::apply("ssdkv-operator"),
            &Patch::Apply(statefulset),
        )
        .await?;
    info!("StatefulSet ensured for {}", name);

    // 6. Update status based on actual StatefulSet state
    let ss_status = ss_api.get(&name).await?;
    let actual_ready = ss_status
        .status
        .as_ref()
        .and_then(|s| s.ready_replicas)
        .unwrap_or(0);
    let desired = cluster.spec.replicas;
    let phase = if actual_ready == desired {
        "Running"
    } else if actual_ready > 0 {
        "Degraded"
    } else {
        "Pending"
    };
    let status = SsdkvClusterStatus {
        phase: Some(phase.to_string()),
        ready_replicas: Some(actual_ready),
        topology_version: Some(1),
    };

    let status_patch = serde_json::json!({
        "apiVersion": "ssdkv.io/v1alpha1",
        "kind": "SsdkvCluster",
        "status": status,
    });

    let cluster_api: Api<SsdkvCluster> = Api::namespaced(client.clone(), &namespace);
    cluster_api
        .patch_status(
            &name,
            &PatchParams::apply("ssdkv-operator"),
            &Patch::Merge(status_patch),
        )
        .await?;

    info!("Reconciliation complete for {}/{}", namespace, name);

    // Requeue after 60 seconds for periodic health check
    Ok(Action::requeue(Duration::from_secs(60)))
}

/// Error policy: log and retry with backoff.
pub fn error_policy(
    _cluster: Arc<SsdkvCluster>,
    err: &kube::Error,
    _ctx: Arc<Context>,
) -> Action {
    error!("Reconcile error: {:?}", err);
    Action::requeue(Duration::from_secs(30))
}
