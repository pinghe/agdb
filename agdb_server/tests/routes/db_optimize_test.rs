use crate::Db;
use crate::TestServer;
use crate::DB_LIST_URI;
use crate::NO_TOKEN;
use agdb::QueryBuilder;
use agdb::QueryType;

#[tokio::test]
async fn optimize() -> anyhow::Result<()> {
    let server = TestServer::new().await?;
    let user = server.init_user().await?;
    let db = server.init_db("mapped", &user).await?;
    let queries: Option<Vec<QueryType>> = Some(vec![QueryBuilder::insert()
        .nodes()
        .count(100)
        .query()
        .into()]);
    server
        .post(&format!("/db/{db}/exec"), &queries, &user.token)
        .await?;
    let original_size = server.get::<Vec<Db>>(DB_LIST_URI, &user.token).await?.1?[0].size;
    let (status, response) = server
        .post::<()>(&format!("/db/{db}/optimize"), &None, &user.token)
        .await?;
    assert_eq!(status, 200);
    let optimized_size = serde_json::from_str::<Db>(&response)?.size;
    assert!(optimized_size < original_size);

    Ok(())
}

#[tokio::test]
async fn permission_denied() -> anyhow::Result<()> {
    let server = TestServer::new().await?;
    let user = server.init_user().await?;
    let other = server.init_user().await?;
    let db = server.init_db("mapped", &user).await?;
    assert_eq!(
        server
            .put::<()>(
                &format!("/db/{db}/user/{}/add?db_role=read", other.name),
                &None,
                &user.token
            )
            .await?,
        201
    );
    let status = server
        .post::<()>(&format!("/db/{db}/optimize"), &None, &other.token)
        .await?
        .0;
    assert_eq!(status, 403);

    Ok(())
}

#[tokio::test]
async fn db_not_found() -> anyhow::Result<()> {
    let server = TestServer::new().await?;
    let user = server.init_user().await?;
    let status = server
        .post::<()>("/db/user/not_found/optimize", &None, &user.token)
        .await?
        .0;
    assert_eq!(status, 404);
    Ok(())
}

#[tokio::test]
async fn no_token() -> anyhow::Result<()> {
    let server = TestServer::new().await?;
    let status = server
        .post::<()>("/db/user/not_found/optimize", &None, NO_TOKEN)
        .await?
        .0;
    assert_eq!(status, 401);
    Ok(())
}