//! SSD-KV Kubernetes Operator
//!
//! Watches SsdkvCluster custom resources and manages:
//! - StatefulSets with stable pod identities
//! - Headless Services for peer discovery
//! - ConfigMaps for topology distribution
//! - Scaling and rebalancing operations

use std::sync::Arc;

use futures::StreamExt;
use kube::runtime::controller::Action;
use kube::runtime::Controller;
use kube::{Api, Client};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod controller;
mod resources;
mod shard_manager;

use controller::{reconcile, error_policy, Context, SsdkvCluster};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!("SSD-KV Operator starting...");

    let client = Client::try_default().await?;
    let clusters: Api<SsdkvCluster> = Api::all(client.clone());

    let context = Arc::new(Context {
        client: client.clone(),
    });

    info!("Starting controller...");

    Controller::new(clusters, Default::default())
        .run(reconcile, error_policy, context)
        .for_each(|result| async move {
            match result {
                Ok((_obj, action)) => {
                    info!("Reconciled: requeue after {:?}", action);
                }
                Err(e) => {
                    error!("Reconcile error: {:?}", e);
                }
            }
        })
        .await;

    Ok(())
}
