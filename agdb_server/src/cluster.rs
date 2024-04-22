use crate::config::Config;
use crate::db_pool::DbPool;
use crate::server_error::ServerResult;
use agdb::StableHash;
use agdb_api::AgdbApi;
use agdb_api::ClusterStatus;
use agdb_api::ReqwestClient;
use agdb_api::Vote;
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

pub(crate) struct ClusterImpl {
    db_pool: DbPool,
    local_address: String,
    user: String,
    password: String,
    nodes: Vec<ClusterNode>,
    hash: u64,
}

impl ClusterImpl {
    pub(crate) fn local_status(&self) -> ClusterStatus {
        ClusterStatus {
            address: self.local_address.clone(),
            cluster_hash: self.hash,
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

pub(crate) fn new(config: &Config, db_pool: DbPool) -> ServerResult<Cluster> {
    let mut nodes = vec![];

    for node in &config.cluster.nodes {
        if node != &config.cluster.local_address {
            nodes.push(Arc::new(RwLock::new(ClusterNodeImpl {
                api: ClusterApi::new(ReqwestClient::new(), node.as_str()),
                status: ClusterStatus {
                    address: node.as_str().to_string(),
                    cluster_hash: 0,
                    leader: false,
                    term: 0,
                    commit: 0,
                },
            })));
        }
    }

    let mut sorted_cluster: Vec<String> = config
        .cluster
        .nodes
        .iter()
        .map(|url| url.to_string())
        .collect();
    sorted_cluster.push(config.cluster.local_address.to_string());
    sorted_cluster.sort();
    tracing::info!("sorted_cluster: {:?}", sorted_cluster);
    let cluster_hash = sorted_cluster.stable_hash();

    Ok(Cluster::new(ClusterImpl {
        db_pool,
        local_address: config.cluster.local_address.as_str().to_string(),
        user: config.cluster.user.clone(),
        password: config.cluster.password.clone(),
        nodes,
        hash: cluster_hash,
    }))
}

pub(crate) async fn cluster_status(cluster: Cluster) -> ServerResult {
    let mut tasks = JoinSet::new();

    for node in &cluster.nodes {
        let node = node.clone();
        tasks.spawn(async move {
            let status = node.read().await.api.status().await;
            if let Ok((_, status)) = status {
                node.write().await.status = status[0].clone();
            }
        });
    }

    while tasks.join_next().await.is_some() {}

    Ok(())
}

pub(crate) async fn election(cluster: Cluster, shutdown_signal: Arc<AtomicBool>) -> ServerResult {
    let mut tasks = JoinSet::new();
    let cluster_info = cluster.db_pool.cluster_info().await?;

    for node in &cluster.nodes {
        let node = node.clone();
        let c = cluster.clone();

        tasks.spawn(async move {
            node.write()
                .await
                .api
                .user_login(&c.user, &c.password)
                .await?;

            node.read()
                .await
                .api
                .vote(c.hash, cluster_info.term, 0, 0, 0)
                .await
        });
    }

    while !shutdown_signal.load(Ordering::Relaxed) || !tasks.is_empty() {
        if let Some(vote) = tasks.try_join_next() {
            let (status, vote) = vote??;

            if status == 200 {
                match vote {
                    Vote::Approve => {}
                    Vote::ClusterHashMismatch(hash) => {}
                    Vote::CommitHashMismatch(hash) => {}
                    Vote::OldCommit(commit) => {}
                    Vote::AlreadyVoted(term) => {}
                }
            }
        }
    }

    Ok(())
}

async fn run_cluster(cluster: Cluster, shutdown_signal: Arc<AtomicBool>) -> ServerResult<()> {
    if cluster.nodes.is_empty() {
        return Ok(());
    }

    election(cluster.clone(), shutdown_signal.clone()).await?;

    while !shutdown_signal.load(Ordering::Relaxed) {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    Ok(())
}

pub(crate) async fn start_with_shutdown(
    cluster: Cluster,
    mut shutdown_receiver: broadcast::Receiver<()>,
) {
    let shutdown_signal = Arc::new(AtomicBool::new(false));
    let cluster_handle = tokio::spawn(run_cluster(cluster.clone(), shutdown_signal.clone()));

    tokio::select! {
        _ = signal::ctrl_c() => {},
        _ = shutdown_receiver.recv() => {},
    }

    shutdown_signal.store(true, Ordering::Relaxed);
    let _ = cluster_handle.await;
}
