use crate::config::Config;
use crate::server_error::ServerResult;
use agdb::StableHash;
use agdb_api::AgdbApi;
use agdb_api::ClusterStatus;
use agdb_api::ReqwestClient;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tokio::task::JoinSet;

pub(crate) type Cluster = Arc<ClusterImpl>;

type ClusterApi = AgdbApi<ReqwestClient>;

pub(crate) struct ClusterNodeImpl {
    api: ClusterApi,
    status: ClusterStatus,
}

type ClusterNode = Arc<RwLock<ClusterNodeImpl>>;

pub(crate) enum ClusterState {
    Unititialized,
    Established,
    Follower,
    Leader,
}

pub(crate) enum ClusterEvent {}

pub(crate) struct ClusterImpl {
    local_address: String,
    nodes: Vec<ClusterNode>,
    pub(crate) cluster_hash: u64,
    //events: Arc<RwLock<VecDeque<ClusterEvent>>>,
}

impl ClusterImpl {
    pub(crate) fn local_status(&self) -> ClusterStatus {
        ClusterStatus {
            address: self.local_address.clone(),
            cluster_hash: self.cluster_hash,
            leader: false,
            term: 0,
            commit: 0,
        }
    }

    pub(crate) async fn statuses(&self) -> Vec<ClusterStatus> {
        let mut statuses = Vec::with_capacity(self.nodes.len());

        for node in &self.nodes {
            statuses.push(node.read().await.status.clone());
        }

        statuses
    }
}

pub(crate) fn new(config: &Config) -> ServerResult<Cluster> {
    let local_address = format!("{}:{}", config.host, config.port);
    let mut nodes = vec![];

    for node in &config.cluster {
        if node != &local_address {
            let (host, port) = node.split_once(':').ok_or(format!(
                "Invalid cluster node address (must be host:port): {}",
                node
            ))?;

            nodes.push(Arc::new(RwLock::new(ClusterNodeImpl {
                api: ClusterApi::new(ReqwestClient::new(), host, port.parse()?),
                status: ClusterStatus {
                    address: node.clone(),
                    cluster_hash: 0,
                    leader: false,
                    term: 0,
                    commit: 0,
                },
            })));
        }
    }

    let mut sorted_cluster: Vec<String> =
        config.cluster.iter().map(|url| url.to_string()).collect();
    sorted_cluster.sort();
    let cluster_hash = sorted_cluster.stable_hash();

    Ok(Cluster::new(ClusterImpl {
        local_address,
        nodes,
        cluster_hash,
        //events: Arc::new(RwLock::new(VecDeque::new())),
    }))
}

pub(crate) async fn cluster_status(cluster: Cluster) -> ServerResult {
    let mut tasks = JoinSet::new();

    for node in &cluster.nodes {
        let node = node.clone();
        tasks.spawn(async move {
            tracing::info!("Getting cluster status...");
            let status = node.read().await.api.status().await;
            tracing::info!("Got status: {:?}", status);
            if let Ok((_, status)) = status {
                tracing::info!("Cluster status: {:?}", status[0]);
                node.write().await.status = status[0].clone();
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    Ok(())
}

async fn start_cluster(cluster: Cluster, shutdown_signal: Arc<AtomicBool>) -> ServerResult<()> {
    if cluster.nodes.is_empty() {
        return Ok(());
    }

    // let mut state = ClusterState::Unititialized;
    let init_handle = tokio::spawn(cluster_status(cluster));

    while !shutdown_signal.load(Ordering::Relaxed) {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    init_handle.await??;

    Ok(())
}

pub(crate) async fn start_with_shutdown(
    cluster: Cluster,
    mut shutdown_receiver: broadcast::Receiver<()>,
) {
    let shutdown_signal = Arc::new(AtomicBool::new(false));
    let cluster_handle = tokio::spawn(start_cluster(cluster.clone(), shutdown_signal.clone()));

    tokio::select! {
        _ = signal::ctrl_c() => {},
        _ = shutdown_receiver.recv() => {},
    }

    shutdown_signal.store(true, Ordering::Relaxed);
    let _ = cluster_handle.await;
}
