pub(crate) mod admin;
pub(crate) mod cluster;
pub(crate) mod db;
pub(crate) mod user;

use crate::cluster::Cluster;
use crate::server_error::ServerResult;
use agdb_api::ServerStatus;
use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;

#[utoipa::path(get,
    path = "/api/v1/status",
    responses(
         (status = 200, description = "Server is ready", body = ClusterStatus),
    )
)]
pub(crate) async fn status(
    State(cluster): State<Cluster>,
) -> ServerResult<(StatusCode, Json<ServerStatus>)> {
    Ok((StatusCode::OK, Json(cluster.local_status().await?)))
}

pub(crate) async fn test_error() -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}
