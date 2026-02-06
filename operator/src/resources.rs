//! Kubernetes resource builders for SSD-KV cluster components.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, Container, ContainerPort, EnvVar, PersistentVolumeClaim,
    PodSpec, PodTemplateSpec, Service, ServicePort, ServiceSpec, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::ResourceExt;

use crate::controller::SsdkvCluster;
use crate::shard_manager::ShardMap;

fn cluster_labels(cluster: &SsdkvCluster) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert("app".to_string(), "ssd-kv".to_string());
    labels.insert(
        "ssdkv.io/cluster".to_string(),
        cluster.name_any(),
    );
    labels
}

/// Builds the headless Service for StatefulSet pod DNS.
pub fn build_headless_service(cluster: &SsdkvCluster) -> Service {
    let name = format!("{}-headless", cluster.name_any());
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let labels = cluster_labels(cluster);

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            selector: Some(labels),
            ports: Some(vec![
                ServicePort {
                    name: Some("redis".to_string()),
                    port: cluster.spec.redis_port,
                    ..Default::default()
                },
                ServicePort {
                    name: Some("cluster".to_string()),
                    port: cluster.spec.cluster_port,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Builds the client-facing Service (load balanced across all nodes).
pub fn build_client_service(cluster: &SsdkvCluster) -> Service {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let labels = cluster_labels(cluster);

    Service {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(labels),
            ports: Some(vec![ServicePort {
                name: Some("redis".to_string()),
                port: cluster.spec.redis_port,
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Builds the topology ConfigMap containing shard-to-node mapping.
pub fn build_topology_configmap(cluster: &SsdkvCluster, shard_map: &ShardMap) -> ConfigMap {
    let name = format!("{}-topology", cluster.name_any());
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let labels = cluster_labels(cluster);

    let mut data = BTreeMap::new();
    data.insert("total_nodes".to_string(), cluster.spec.replicas.to_string());
    data.insert(
        "replication_factor".to_string(),
        cluster.spec.replication_factor.to_string(),
    );
    data.insert(
        "shard_map.json".to_string(),
        serde_json::to_string(&shard_map.assignments).unwrap_or_default(),
    );

    // Generate peer list
    let headless_svc = format!("{}-headless", cluster.name_any());
    let peers: Vec<String> = (0..cluster.spec.replicas)
        .map(|i| {
            format!(
                "{}-{}:{}.{}.svc.cluster.local:{}",
                cluster.name_any(),
                i,
                headless_svc,
                namespace,
                cluster.spec.cluster_port,
            )
        })
        .collect();
    data.insert("peers".to_string(), peers.join(","));

    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name),
            namespace: Some(namespace),
            labels: Some(labels),
            ..Default::default()
        },
        data: Some(data),
        ..Default::default()
    }
}

/// Builds the StatefulSet for SSD-KV nodes.
pub fn build_statefulset(cluster: &SsdkvCluster) -> StatefulSet {
    let name = cluster.name_any();
    let namespace = cluster.namespace().unwrap_or_else(|| "default".to_string());
    let labels = cluster_labels(cluster);
    let headless_svc = format!("{}-headless", name);

    // Build peer list for --cluster-peers
    let peers: Vec<String> = (0..cluster.spec.replicas)
        .map(|i| {
            format!(
                "{}-{}.{}.{}.svc.cluster.local:{}",
                name, i, headless_svc, namespace, cluster.spec.cluster_port
            )
        })
        .collect();
    let peers_str = peers.join(",");

    let container = Container {
        name: "ssd-kv".to_string(),
        image: Some(cluster.spec.image.clone()),
        ports: Some(vec![
            ContainerPort {
                name: Some("redis".to_string()),
                container_port: cluster.spec.redis_port,
                ..Default::default()
            },
            ContainerPort {
                name: Some("cluster".to_string()),
                container_port: cluster.spec.cluster_port,
                ..Default::default()
            },
        ]),
        env: Some(vec![
            EnvVar {
                name: "POD_NAME".to_string(),
                value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                    field_ref: Some(k8s_openapi::api::core::v1::ObjectFieldSelector {
                        field_path: "metadata.name".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ]),
        command: Some(vec!["/bin/sh".to_string()]),
        args: Some(vec![
            "-c".to_string(),
            format!(
                concat!(
                    "NODE_ID=$(echo $POD_NAME | rev | cut -d'-' -f1 | rev) && ",
                    "exec ssd-kv ",
                    "--bind 0.0.0.0:{} ",
                    "--data-dir /data ",
                    "--cluster-mode ",
                    "--node-id $NODE_ID ",
                    "--total-nodes {} ",
                    "--replication-factor {} ",
                    "--cluster-port {} ",
                    "--cluster-peers {}"
                ),
                cluster.spec.redis_port,
                cluster.spec.replicas,
                cluster.spec.replication_factor,
                cluster.spec.cluster_port,
                peers_str,
            ),
        ]),
        volume_mounts: Some(vec![VolumeMount {
            name: "data".to_string(),
            mount_path: "/data".to_string(),
            ..Default::default()
        }]),
        ..Default::default()
    };

    // Build PVC template
    let mut pvc_resources = BTreeMap::new();
    pvc_resources.insert(
        "storage".to_string(),
        Quantity(cluster.spec.storage.size.clone()),
    );

    let pvc_template = PersistentVolumeClaim {
        metadata: ObjectMeta {
            name: Some("data".to_string()),
            ..Default::default()
        },
        spec: Some(k8s_openapi::api::core::v1::PersistentVolumeClaimSpec {
            access_modes: Some(vec!["ReadWriteOnce".to_string()]),
            resources: Some(k8s_openapi::api::core::v1::VolumeResourceRequirements {
                requests: Some(pvc_resources),
                ..Default::default()
            }),
            storage_class_name: cluster.spec.storage.storage_class.clone(),
            ..Default::default()
        }),
        ..Default::default()
    };

    StatefulSet {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace),
            labels: Some(labels.clone()),
            ..Default::default()
        },
        spec: Some(StatefulSetSpec {
            replicas: Some(cluster.spec.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            service_name: headless_svc,
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    ..Default::default()
                }),
            },
            volume_claim_templates: Some(vec![pvc_template]),
            ..Default::default()
        }),
        ..Default::default()
    }
}
