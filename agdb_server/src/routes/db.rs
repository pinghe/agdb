pub(crate) mod user;

use crate::action::ClusterActionResult;
use crate::action::db_add::DbAdd;
use crate::action::db_backup::DbBackup;
use crate::action::db_clear::DbClear;
use crate::action::db_convert::DbConvert;
use crate::action::db_copy::DbCopy;
use crate::action::db_delete::DbDelete;
use crate::action::db_exec::DbExec;
use crate::action::db_optimize::DbOptimize;
use crate::action::db_remove::DbRemove;
use crate::action::db_rename::DbRename;
use crate::action::db_restore::DbRestore;
use crate::cluster::Cluster;
use crate::db_pool::DbPool;
use crate::error_code::ErrorCode;
use crate::server_db::ServerDb;
use crate::server_error::ServerError;
use crate::server_error::ServerResponse;
use crate::server_error::permission_denied;
use crate::user_id::UserId;
use crate::utilities::required_role;
use agdb_api::DbAudit;
use agdb_api::DbResource;
use agdb_api::DbType;
use agdb_api::DbUserRole;
use agdb_api::Queries;
use agdb_api::QueriesResults;
use agdb_api::ServerDatabase;
use axum::Json;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa::ToSchema;

#[derive(Deserialize, IntoParams, ToSchema, agdb::api::ApiDef)]
#[into_params(parameter_in = Query)]
pub struct ServerDatabaseRename {
    pub new_db: String,
}

#[derive(Deserialize, IntoParams, ToSchema, agdb::api::ApiDef)]
#[into_params(parameter_in = Query)]
pub(crate) struct DbTypeParam {
    pub(crate) db_type: DbType,
}

#[derive(Deserialize, IntoParams, ToSchema, agdb::api::ApiDef)]
#[into_params(parameter_in = Query)]
pub struct ServerDatabaseResource {
    pub resource: DbResource,
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/add",
    operation_id = "db_add",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "user name"),
        ("db" = String, Path, description = "db name"),
        DbTypeParam,
    ),
    responses(
         (status = 201, description = "db added"),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "cannot add db to another user"),
         (status = 465, description = "db already exists"),
         (status = 467, description = "db invalid"),
    )
)]
pub(crate) async fn add(
    user: UserId,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
    request: Query<DbTypeParam>,
) -> ServerResponse<impl IntoResponse> {
    let username = server_db.user_name(user.0).await?;

    if username != owner {
        return Err(ServerError::new(
            StatusCode::FORBIDDEN,
            "cannot add db to another user",
        ));
    }

    if server_db
        .find_user_db_id(user.0, &owner, &db)
        .await?
        .is_some()
    {
        return Err(ErrorCode::DbExists.into());
    }

    let (commit_index, _result) = cluster
        .exec(DbAdd {
            owner,
            db,
            db_type: request.db_type,
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        [("commit-index", commit_index.to_string())],
    ))
}

#[utoipa::path(get,
    path = "/api/v1/db/{owner}/{db}/audit",
    operation_id = "db_audit",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "user name"),
        ("db" = String, Path, description = "db name")
    ),
    responses(
         (status = 200, description = "ok", body = DbAudit),
         (status = 401, description = "unauthorized"),
         (status = 404, description = "user / db not found"),
    )
)]
pub(crate) async fn audit(
    user: UserId,
    State(db_pool): State<DbPool>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
) -> ServerResponse<(StatusCode, Json<DbAudit>)> {
    server_db.user_db_id(user.0, &owner, &db).await?;

    Ok((StatusCode::OK, Json(db_pool.audit(&owner, &db).await?)))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/backup",
    operation_id = "db_backup",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "user name"),
        ("db" = String, Path, description = "db name"),
    ),
    responses(
         (status = 201, description = "backup created"),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "must be a db admin / memory db cannot have backup"),
         (status = 404, description = "user / db not found"),
    )
)]
pub(crate) async fn backup(
    user: UserId,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
) -> ServerResponse<impl IntoResponse> {
    let db_id = server_db.user_db_id(user.0, &owner, &db).await?;

    if !server_db.is_db_admin(user.0, db_id).await? {
        return Err(permission_denied("admin only"));
    }

    let (commit_index, _result) = cluster.exec(DbBackup { owner, db }).await?;

    Ok((
        StatusCode::CREATED,
        [("commit-index", commit_index.to_string())],
    ))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/clear",
    operation_id = "db_clear",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "user name"),
        ("db" = String, Path, description = "db name"),
        ServerDatabaseResource
    ),
    responses(
         (status = 201, description = "db resource(s) cleared", body = ServerDatabase),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "must be a db admin"),
         (status = 404, description = "user / db not found"),
    )
)]
pub(crate) async fn clear(
    user: UserId,
    State(cluster): State<Cluster>,
    State(db_pool): State<DbPool>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
    request: Query<ServerDatabaseResource>,
) -> ServerResponse<impl IntoResponse> {
    let db_id = server_db.user_db_id(user.0, &owner, &db).await?;
    let role = server_db.user_db_role(user.0, &owner, &db).await?;

    if !server_db.is_db_admin(user.0, db_id).await? {
        return Err(permission_denied("admin only"));
    }

    let (commit_index, _result) = cluster
        .exec(DbClear {
            owner: owner.clone(),
            db: db.clone(),
            resource: request.resource,
        })
        .await?;

    let size = db_pool.db_size(&owner, &db).await.unwrap_or(0);
    let database = server_db.user_db(user.0, &owner, &db).await?;
    let db = ServerDatabase {
        db,
        owner,
        db_type: database.db_type,
        role,
        backup: database.backup,
        size,
    };

    Ok((
        StatusCode::OK,
        [("commit-index", commit_index.to_string())],
        Json(db),
    ))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/convert",
    operation_id = "db_convert",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "user name"),
        ("db" = String, Path, description = "db name"),
        DbTypeParam,
    ),
    responses(
         (status = 201, description = "db type changes"),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "must be a db admin"),
         (status = 404, description = "user / db not found"),
    )
)]
pub(crate) async fn convert(
    user: UserId,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
    request: Query<DbTypeParam>,
) -> ServerResponse<impl IntoResponse> {
    let database = server_db.user_db(user.0, &owner, &db).await?;

    if !server_db
        .is_db_admin(user.0, database.db_id.unwrap_or_default())
        .await?
    {
        return Err(permission_denied("admin only"));
    }

    if database.db_type == request.db_type {
        return Ok((StatusCode::CREATED, [("commit-index", String::new())]));
    }

    let (commit_index, _result) = cluster
        .exec(DbConvert {
            owner,
            db,
            db_type: request.db_type,
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        [("commit-index", commit_index.to_string())],
    ))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/copy",
    operation_id = "db_copy",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "db owner user name"),
        ("db" = String, Path, description = "db name"),
        ServerDatabaseRename
    ),
    responses(
         (status = 201, description = "db copied"),
         (status = 401, description = "unauthorized"),
         (status = 404, description = "user / db not found"),
         (status = 465, description = "target db exists"),
         (status = 467, description = "invalid db"),
    )
)]
pub(crate) async fn copy(
    user: UserId,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
    request: Query<ServerDatabaseRename>,
) -> ServerResponse<impl IntoResponse> {
    let db_type = server_db.user_db(user.0, &owner, &db).await?.db_type;
    let username = server_db.user_name(user.0).await?;

    if server_db
        .find_user_db_id(user.0, &username, &request.new_db)
        .await?
        .is_some()
    {
        return Err(ErrorCode::DbExists.into());
    }

    let (commit_index, _result) = cluster
        .exec(DbCopy {
            owner,
            db,
            new_owner: username,
            new_db: request.new_db.clone(),
            db_type,
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        [("commit-index", commit_index.to_string())],
    ))
}

#[utoipa::path(delete,
    path = "/api/v1/db/{owner}/{db}/delete",
    operation_id = "db_delete",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "db owner user name"),
        ("db" = String, Path, description = "db name"),
    ),
    responses(
         (status = 204, description = "db deleted"),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "user must be a db owner"),
         (status = 404, description = "db not found"),
    )
)]
pub(crate) async fn delete(
    user: UserId,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
) -> ServerResponse<impl IntoResponse> {
    let username = server_db.user_name(user.0).await?;

    if owner != username {
        return Err(permission_denied("owner only"));
    }

    let _ = server_db.user_db_id(user.0, &owner, &db).await?;

    let (commit_index, _result) = cluster.exec(DbDelete { owner, db }).await?;

    Ok((
        StatusCode::NO_CONTENT,
        [("commit-index", commit_index.to_string())],
    ))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/exec",
    operation_id = "db_exec",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "db owner user name"),
        ("db" = String, Path, description = "db name"),
    ),
    request_body = Queries,
    responses(
         (status = 200, description = "ok", body = QueriesResults),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "mutable queries not allowed"),
         (status = 404, description = "db not found"),
    )
)]
pub(crate) async fn exec(
    user: UserId,
    State(db_pool): State<DbPool>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
    Json(queries): Json<Queries>,
) -> ServerResponse<impl IntoResponse> {
    let _ = server_db.user_db_id(user.0, &owner, &db).await?;
    let required_role = required_role(&queries);
    if required_role != DbUserRole::Read {
        return Err(permission_denied(
            "mutable queries not allowed, use exec_mut endpoint",
        ));
    }
    let results = db_pool.exec(&owner, &db, queries).await?;
    Ok((StatusCode::OK, Json(QueriesResults(results))))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/exec_mut",
    operation_id = "db_exec_mut",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "db owner user name"),
        ("db" = String, Path, description = "db name"),
    ),
    request_body = Queries,
    responses(
         (status = 200, description = "ok", body = QueriesResults),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "must have at least write role"),
         (status = 404, description = "db not found"),
    )
)]
pub(crate) async fn exec_mut(
    user: UserId,
    State(db_pool): State<DbPool>,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
    Json(queries): Json<Queries>,
) -> ServerResponse<impl IntoResponse> {
    let role = server_db.user_db_role(user.0, &owner, &db).await?;
    let required_role = required_role(&queries);

    if role == DbUserRole::Read {
        return Err(permission_denied("write rights required"));
    }

    let (commit_index, results) = if required_role == DbUserRole::Read {
        (0, db_pool.exec(&owner, &db, queries).await?)
    } else {
        let username = server_db.user_name(user.0).await?;
        let mut index = 0;
        let mut results = Vec::new();

        if let (i, ClusterActionResult::QueryResults(r)) = cluster
            .exec(DbExec {
                user: username,
                owner,
                db,
                queries,
            })
            .await?
        {
            index = i;
            results = r;
        }

        (index, results)
    };

    Ok((
        StatusCode::OK,
        [("commit-index", commit_index.to_string())],
        Json(QueriesResults(results)),
    ))
}

#[utoipa::path(get,
    path = "/api/v1/db/list",
    operation_id = "db_list",
    tag = "agdb",
    security(("Token" = [])),
    responses(
         (status = 200, description = "ok", body = Vec<ServerDatabase>),
         (status = 401, description = "unauthorized"),
    )
)]
pub(crate) async fn list(
    user: UserId,
    State(db_pool): State<DbPool>,
    State(server_db): State<ServerDb>,
) -> ServerResponse<(StatusCode, Json<Vec<ServerDatabase>>)> {
    let databases = server_db.user_dbs(user.0).await?;
    let mut sizes = Vec::with_capacity(databases.len());

    for (_, db) in &databases {
        sizes.push(db_pool.db_size(&db.owner, &db.db).await.unwrap_or(0));
    }

    let dbs = databases
        .into_iter()
        .zip(sizes)
        .filter_map(|((role, db), size)| {
            if size != 0 {
                return Some(ServerDatabase {
                    db: db.db,
                    owner: db.owner,
                    db_type: db.db_type,
                    role,
                    backup: db.backup,
                    size,
                });
            }
            None
        })
        .collect();

    Ok((StatusCode::OK, Json(dbs)))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/optimize",
    operation_id = "db_optimize",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "user name"),
        ("db" = String, Path, description = "db name"),
    ),
    responses(
         (status = 200, description = "ok", body = ServerDatabase),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "must have write permissions"),
         (status = 404, description = "db not found"),
    )
)]
pub(crate) async fn optimize(
    user: UserId,
    State(db_pool): State<DbPool>,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
) -> ServerResponse<impl IntoResponse> {
    let database = server_db.user_db(user.0, &owner, &db).await?;
    let role = server_db.user_db_role(user.0, &owner, &db).await?;

    if role == DbUserRole::Read {
        return Err(permission_denied("write rights required"));
    }

    let (commit_index, _result) = cluster
        .exec(DbOptimize {
            owner: owner.clone(),
            db: db.clone(),
        })
        .await?;
    let size = db_pool.db_size(&owner, &db).await?;

    Ok((
        StatusCode::OK,
        [("commit-index", commit_index.to_string())],
        Json(ServerDatabase {
            db: db.to_string(),
            owner,
            db_type: database.db_type,
            role,
            backup: database.backup,
            size,
        }),
    ))
}

#[utoipa::path(delete,
    path = "/api/v1/db/{owner}/{db}/remove",
    operation_id = "db_remove",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "db owner user name"),
        ("db" = String, Path, description = "db name"),
    ),
    responses(
         (status = 204, description = "db removed"),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "user must be a db owner"),
         (status = 404, description = "db not found"),
    )
)]
pub(crate) async fn remove(
    user: UserId,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
) -> ServerResponse<impl IntoResponse> {
    let user_name = server_db.user_name(user.0).await?;

    if owner != user_name {
        return Err(permission_denied("owner only"));
    }

    let _ = server_db.user_db_id(user.0, &owner, &db).await?;

    let (commit_index, _result) = cluster.exec(DbRemove { owner, db }).await?;

    Ok((
        StatusCode::NO_CONTENT,
        [("commit-index", commit_index.to_string())],
    ))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/rename",
    operation_id = "db_rename",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "db owner user name"),
        ("db" = String, Path, description = "db name"),
        ServerDatabaseRename
    ),
    responses(
         (status = 201, description = "db renamed"),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "user must be a db owner"),
         (status = 404, description = "user / db not found"),
         (status = 465, description = "target db exists"),
         (status = 467, description = "invalid db"),
    )
)]
pub(crate) async fn rename(
    user: UserId,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
    request: Query<ServerDatabaseRename>,
) -> ServerResponse<impl IntoResponse> {
    let username = server_db.user_name(user.0).await?;

    if owner != username {
        return Err(permission_denied("owner only"));
    }

    if request.new_db == db {
        return Ok((StatusCode::CREATED, [("commit-index", String::new())]));
    }

    if server_db
        .find_user_db_id(user.0, &username, &request.new_db)
        .await?
        .is_some()
    {
        return Err(ErrorCode::DbExists.into());
    }

    let (commit_index, _result) = cluster
        .exec(DbRename {
            owner,
            db,
            new_owner: username,
            new_db: request.new_db.to_string(),
        })
        .await?;

    Ok((
        StatusCode::CREATED,
        [("commit-index", commit_index.to_string())],
    ))
}

#[utoipa::path(post,
    path = "/api/v1/db/{owner}/{db}/restore",
    operation_id = "db_restore",
    tag = "agdb",
    security(("Token" = [])),
    params(
        ("owner" = String, Path, description = "user name"),
        ("db" = String, Path, description = "db name"),
    ),
    responses(
         (status = 201, description = "db restored"),
         (status = 401, description = "unauthorized"),
         (status = 403, description = "must be a db admin"),
         (status = 404, description = "backup not found"),
    )
)]
pub(crate) async fn restore(
    user: UserId,
    State(cluster): State<Cluster>,
    State(server_db): State<ServerDb>,
    Path((owner, db)): Path<(String, String)>,
) -> ServerResponse<impl IntoResponse> {
    let db_id = server_db.user_db_id(user.0, &owner, &db).await?;

    if !server_db.is_db_admin(user.0, db_id).await? {
        return Err(permission_denied("admin only"));
    }

    let (commit_index, _result) = cluster.exec(DbRestore { owner, db }).await?;

    Ok((
        StatusCode::CREATED,
        [("commit-index", commit_index.to_string())],
    ))
}
