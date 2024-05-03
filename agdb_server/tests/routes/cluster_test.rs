use crate::ClusterConfig;
use crate::Config;
use crate::TestServerImpl;
use crate::ADMIN;
use crate::CLUSTER_ADMIN;
use crate::HOST;
use crate::SERVER_DATA_DIR;
use agdb_api::AgdbApi;
use agdb_api::ReqwestClient;

#[tokio::test]
async fn db_cluster_established() -> anyhow::Result<()> {
    let port1 = TestServerImpl::next_port();
    let port2 = TestServerImpl::next_port();
    let port3 = TestServerImpl::next_port();

    let config1 = Config {
        bind: format!("{HOST}:{port1}"),
        admin: ADMIN.into(),
        data_dir: SERVER_DATA_DIR.into(),
        cluster: ClusterConfig {
            local_address: format!("http://{HOST}:{port1}"),
            user: CLUSTER_ADMIN.into(),
            password: CLUSTER_ADMIN.into(),
            nodes: vec![
                format!("http://{HOST}:{port2}"),
                format!("http://{HOST}:{port3}"),
            ],
        },
    };

    let config2 = Config {
        bind: format!("{HOST}:{port2}"),
        admin: ADMIN.into(),
        data_dir: SERVER_DATA_DIR.into(),
        cluster: ClusterConfig {
            local_address: format!("http://{HOST}:{port2}"),
            user: CLUSTER_ADMIN.into(),
            password: CLUSTER_ADMIN.into(),
            nodes: vec![
                format!("http://{HOST}:{port1}"),
                format!("http://{HOST}:{port3}"),
            ],
        },
    };

    let config3 = Config {
        bind: format!("{HOST}:{port3}"),
        admin: ADMIN.into(),
        data_dir: SERVER_DATA_DIR.into(),
        cluster: ClusterConfig {
            local_address: format!("http://{HOST}:{port3}"),
            user: CLUSTER_ADMIN.into(),
            password: CLUSTER_ADMIN.into(),
            nodes: vec![
                format!("http://{HOST}:{port1}"),
                format!("http://{HOST}:{port2}"),
            ],
        },
    };

    let server1 = TestServerImpl::with_config(config1).await?;
    let server2 = TestServerImpl::with_config(config2).await?;
    let server3 = TestServerImpl::with_config(config3).await?;

    let client1 = AgdbApi::new(ReqwestClient::new(), &server1.address);
    let client2 = AgdbApi::new(ReqwestClient::new(), &server2.address);
    let client3 = AgdbApi::new(ReqwestClient::new(), &server3.address);

    let mut status1 = client1.cluster_status().await?;
    let mut status2 = client2.cluster_status().await?;
    let mut status3 = client3.cluster_status().await?;

    assert_eq!(status1.0, 200);
    assert_eq!(status2.0, 200);
    assert_eq!(status3.0, 200);

    status1.1.sort_by(|a, b| a.address.cmp(&b.address));
    status2.1.sort_by(|a, b| a.address.cmp(&b.address));
    status3.1.sort_by(|a, b| a.address.cmp(&b.address));

    assert_eq!(status1.1, status2.1);
    assert_eq!(status1.1, status3.1);

    //assert!(status1.1.iter().any(|s| s.leader));

    Ok(())
}
