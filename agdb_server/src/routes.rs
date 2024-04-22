pub(crate) mod admin;
pub(crate) mod db;
pub(crate) mod user;

use crate::cluster;
use crate::cluster::Cluster;
use crate::server_error::ServerResult;
use agdb_api::ClusterStatus;
use agdb_api::StatusParams;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;

#[utoipa::path(get,
    path = "/api/v1/status",
    params(
        ("cluster" = bool, description = "get cluster status"),
    ),
    responses(
         (status = 200, description = "Server is ready", body = Vec<ClusterStatus>),
    )
)]
pub(crate) async fn status(
    State(cluster): State<Cluster>,
    Query(status_params): Query<StatusParams>,
) -> ServerResult<(StatusCode, Json<Vec<ClusterStatus>>)> {
    let statuses = if status_params.cluster.unwrap_or_default() {
        cluster::cluster_status(cluster.clone()).await?;
        let cluster_statuses = cluster.statuses().await;
        let mut statuses = Vec::with_capacity(cluster_statuses.len() + 1);
        statuses.push(cluster.local_status());
        statuses.extend(cluster_statuses);
        statuses
    } else {
        vec![cluster.local_status()]
    };

    Ok((StatusCode::OK, Json(statuses)))
}

pub(crate) async fn test_error() -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}
