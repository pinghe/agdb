pub(crate) mod db;
pub(crate) mod user;

use crate::user_id::AdminId;
use axum::extract::State;
use axum::http::StatusCode;
use tokio::sync::broadcast::Sender;

#[utoipa::path(get,
    path = "/api/v1/admin/shutdown",
    security(("Token" = [])),
    responses(
         (status = 200, description = "Server is shutting down"),
    )
)]
pub(crate) async fn shutdown(
    _admin_id: AdminId,
    State(shutdown_sender): State<Sender<()>>,
) -> StatusCode {
    match shutdown_sender.send(()) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use agdb::DbId;

    #[tokio::test]
    async fn shutdown_test() -> anyhow::Result<()> {
        let (shutdown_sender, _shutdown_receiver) = tokio::sync::broadcast::channel::<()>(1);

        let status = shutdown(AdminId(DbId(0)), State(shutdown_sender)).await;

        assert_eq!(status, StatusCode::OK);
        Ok(())
    }

    #[tokio::test]
    async fn bad_shutdown() -> anyhow::Result<()> {
        let shutdown_sender = Sender::<()>::new(1);

        let status = shutdown(AdminId(DbId(0)), State(shutdown_sender)).await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        Ok(())
    }
}
