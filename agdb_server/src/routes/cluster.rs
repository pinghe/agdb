use crate::cluster::cluster_status;
use crate::cluster::Cluster;
use crate::server_error::ServerResult;
use agdb_api::ServerStatus;
use axum::extract::State;
use axum::Json;
use reqwest::StatusCode;

#[utoipa::path(get,
    path = "/api/v1/cluster/status",
    responses(
         (status = 200, description = "Cluster status", body = Vec<ServerStatus>),
    )
)]
pub(crate) async fn status(
    State(cluster): State<Cluster>,
) -> ServerResult<(StatusCode, Json<Vec<ServerStatus>>)> {
    cluster_status(cluster.clone()).await?;
    let cluster_statuses = cluster.statuses().await;
    let mut statuses = Vec::with_capacity(cluster_statuses.len() + 1);
    statuses.push(cluster.local_status().await?);
    statuses.extend(cluster_statuses);

    Ok((StatusCode::OK, Json(statuses)))
}
