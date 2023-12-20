mod routes;

use agdb::QueryResult;
use agdb::QueryType;
use anyhow::anyhow;
use assert_cmd::prelude::*;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::Path;
use std::process::Child;
use std::process::Command;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::Mutex;

pub const USER_CHANGE_PASSWORD_URI: &str = "/user/change_password";
pub const DB_ADD_URI: &str = "/db/add";
pub const DB_DELETE_URI: &str = "/db/delete";
pub const DB_EXEC_URI: &str = "/db/exec";
pub const DB_LIST_URI: &str = "/db/list";
pub const DB_RENAME_URI: &str = "/db/rename";
pub const DB_USER_ADD_URI: &str = "/db/user/add";
pub const DB_USER_LIST_URI: &str = "/db/user/list";
pub const DB_USER_REMOVE_URI: &str = "/db/user/remove";
pub const ADMIN_USER_CREATE_URI: &str = "/admin/user/create";
pub const ADMIN_DB_ADD_URI: &str = "/admin/db/add";
pub const ADMIN_DB_LIST_URI: &str = "/admin/db/list";
pub const ADMIN_DB_DELETE_URI: &str = "/admin/db/delete";
pub const ADMIN_DB_REMOVE_URI: &str = "/admin/db/remove";
pub const ADMIN_DB_USER_ADD_URI: &str = "/admin/db/user/add";
pub const ADMIN_DB_USER_LIST_URI: &str = "/admin/db/user/list";
pub const ADMIN_DB_USER_REMOVE_URI: &str = "/admin/db/user/remove";
pub const ADMIN_USER_LIST_URI: &str = "/admin/user/list";
pub const ADMIN_CHANGE_PASSWORD_URI: &str = "/admin/user/change_password";
pub const DB_REMOVE_URI: &str = "/db/remove";
pub const USER_LOGIN_URI: &str = "/user/login";
pub const SHUTDOWN_URI: &str = "/admin/shutdown";
pub const STATUS_URI: &str = "/status";

pub const NO_TOKEN: &Option<String> = &None;

const BINARY: &str = "agdb_server";
const CONFIG_FILE: &str = "agdb_server.yaml";
const SERVER_DATA_DIR: &str = "agdb_server_data";
const PROTOCOL: &str = "http";
const HOST: &str = "127.0.0.1";
const DEFAULT_PORT: u16 = 3000;
const ADMIN: &str = "admin";
const RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const RETRY_ATTEMPS: u16 = 3;

static MUTEX: OnceLock<Mutex<bool>> = OnceLock::new();
static SERVER: OnceLock<TestServerImpl> = OnceLock::new();
static INSTANCES: AtomicU16 = AtomicU16::new(0);
static PORT: AtomicU16 = AtomicU16::new(DEFAULT_PORT);
static COUNTER: AtomicU16 = AtomicU16::new(1);

#[derive(Serialize, Deserialize)]
pub struct AddUser<'a> {
    pub user: &'a str,
    pub database: &'a str,
    pub role: &'a str,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Db {
    pub name: String,
    pub db_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DbWithSize {
    pub name: String,
    pub db_type: String,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DbWithRole {
    pub name: String,
    pub db_type: String,
    pub role: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize)]
pub struct User<'a> {
    pub name: &'a str,
    pub password: &'a str,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct UserStatus {
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct ChangePassword<'a> {
    pub name: &'a str,
    pub password: &'a str,
    pub new_password: &'a str,
}

pub struct TestServerImpl {
    pub dir: String,
    pub data_dir: String,
    pub port: u16,
    pub process: Child,
    pub admin: String,
    pub admin_password: String,
    pub admin_token: Option<String>,
}

pub struct ServerUser {
    pub name: String,
    pub token: Option<String>,
}

pub struct TestServer {
    server: &'static TestServerImpl,
    client: Client,
}

impl TestServer {
    pub async fn new() -> anyhow::Result<Self> {
        let server = TestServerImpl::new().await?;
        Ok(Self {
            server,
            client: reqwest::Client::new(),
        })
    }

    pub async fn exec(
        &self,
        db: &str,
        queries: &Vec<QueryType>,
        token: &Option<String>,
    ) -> anyhow::Result<Vec<QueryResult>> {
        let (status, response) = self
            .server
            .post(
                &self.client,
                &format!("{DB_EXEC_URI}?db={}", db),
                &queries,
                token,
            )
            .await?;
        assert_eq!(status, 200);
        Ok(serde_json::from_str(&response)?)
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        uri: &str,
        token: &Option<String>,
    ) -> anyhow::Result<(u16, anyhow::Result<T>)> {
        self.server.get(&self.client, uri, token).await
    }

    pub async fn post<T: Serialize>(
        &self,
        uri: &str,
        json: &T,
        token: &Option<String>,
    ) -> anyhow::Result<(u16, String)> {
        self.server.post(&self.client, uri, json, token).await
    }

    pub async fn init_user(&self) -> anyhow::Result<ServerUser> {
        self.server.init_user(&self.client).await
    }

    pub async fn init_db(&self, db_type: &str, server_user: &ServerUser) -> anyhow::Result<String> {
        self.server
            .init_db(&self.client, db_type, server_user)
            .await
    }
}

impl Deref for TestServer {
    type Target = TestServerImpl;

    fn deref(&self) -> &Self::Target {
        self.server
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let remaining = INSTANCES.fetch_sub(1, Ordering::Relaxed);

        if remaining == 1 {
            self.server.shutdown_server().unwrap();

            for _ in 0..RETRY_ATTEMPS {
                if TestServerImpl::remove_dir_if_exists(&self.dir).is_ok() {
                    break;
                } else {
                    std::thread::sleep(RETRY_TIMEOUT);
                }
            }
        }
    }
}

impl TestServerImpl {
    pub async fn new() -> anyhow::Result<&'static Self> {
        let mutex = MUTEX.get_or_init(|| Mutex::new(true));
        let _lock = mutex.lock().await;

        INSTANCES.fetch_add(1, Ordering::Relaxed);

        Ok(if let Some(server) = SERVER.get() {
            server
        } else {
            let server = Self::init().await?;
            SERVER.get_or_init(|| server)
        })
    }

    pub async fn init() -> anyhow::Result<Self> {
        let port = PORT.fetch_add(1, Ordering::Relaxed);
        let dir = format!("{BINARY}.{port}.test");
        let data_dir = format!("{dir}/{SERVER_DATA_DIR}");

        Self::remove_dir_if_exists(&dir)?;
        std::fs::create_dir(&dir)?;

        if port != DEFAULT_PORT {
            let mut config = HashMap::<&str, serde_yaml::Value>::new();
            config.insert("host", HOST.into());
            config.insert("port", port.into());
            config.insert("admin", ADMIN.into());
            config.insert("data_dir", SERVER_DATA_DIR.into());

            let file = std::fs::File::options()
                .create_new(true)
                .write(true)
                .open(Path::new(&dir).join(CONFIG_FILE))?;
            serde_yaml::to_writer(file, &config)?;
        }

        let process = Command::cargo_bin(BINARY)?.current_dir(&dir).spawn()?;
        let client = reqwest::Client::new();

        let mut error = anyhow!("Failed to start server");

        for _ in 0..RETRY_ATTEMPS {
            match client
                .get(format!("{}:{}/api/v1{STATUS_URI}", Self::url_base(), port))
                .send()
                .await
            {
                Ok(_) => {
                    let admin = User {
                        name: ADMIN,
                        password: ADMIN,
                    };
                    let response = client
                        .post(format!(
                            "{}:{}/api/v1{USER_LOGIN_URI}",
                            Self::url_base(),
                            port
                        ))
                        .json(&admin)
                        .send()
                        .await?;
                    let admin_token = Some(response.text().await?);
                    let server = Self {
                        dir,
                        data_dir,
                        port,
                        process,
                        admin: ADMIN.to_string(),
                        admin_password: ADMIN.to_string(),
                        admin_token,
                    };
                    return Ok(server);
                }
                Err(e) => {
                    error = e.into();
                }
            }
            std::thread::sleep(RETRY_TIMEOUT);
        }

        Err(error)
    }

    pub async fn get<T: DeserializeOwned>(
        &self,
        client: &Client,
        uri: &str,
        token: &Option<String>,
    ) -> anyhow::Result<(u16, anyhow::Result<T>)> {
        let mut request = client.get(self.url(uri));

        if let Some(token) = token {
            request = request.bearer_auth(token);
        }

        let response = request.send().await?;
        let status = response.status().as_u16();

        Ok((status, response.json().await.map_err(|e| anyhow!(e))))
    }

    pub async fn init_user(&self, client: &Client) -> anyhow::Result<ServerUser> {
        let name = format!("db_user{}", COUNTER.fetch_add(1, Ordering::Relaxed));
        let user = User {
            name: &name,
            password: &name,
        };
        assert_eq!(
            self.post(client, ADMIN_USER_CREATE_URI, &user, &self.admin_token)
                .await?
                .0,
            201
        );
        let response = self.post(client, USER_LOGIN_URI, &user, &None).await?;
        assert_eq!(response.0, 200);
        Ok(ServerUser {
            name,
            token: Some(response.1),
        })
    }

    pub async fn init_db(
        &self,
        client: &Client,
        db_type: &str,
        server_user: &ServerUser,
    ) -> anyhow::Result<String> {
        let name = format!(
            "{}/db{}",
            server_user.name,
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let db = Db {
            name: name.clone(),
            db_type: db_type.to_string(),
        };
        let (status, _) = self
            .post(client, DB_ADD_URI, &db, &server_user.token)
            .await?;
        assert_eq!(status, 201);
        Ok(name)
    }

    pub async fn post<T: Serialize>(
        &self,
        client: &Client,
        uri: &str,
        json: &T,
        token: &Option<String>,
    ) -> anyhow::Result<(u16, String)> {
        let mut request = client.post(self.url(uri)).json(&json);

        if let Some(token) = token {
            request = request.bearer_auth(token);
        }

        let response = request.send().await?;

        Ok((response.status().as_u16(), response.text().await?))
    }

    fn remove_dir_if_exists(dir: &str) -> anyhow::Result<()> {
        if Path::new(dir).exists() {
            std::fs::remove_dir_all(dir)?;
        }

        Ok(())
    }

    fn shutdown_server(&self) -> anyhow::Result<()> {
        let port = self.port;
        let mut admin = HashMap::<&str, String>::new();
        admin.insert("name", self.admin.clone());
        admin.insert("password", self.admin_password.clone());
        let admin_token = self.admin_token.clone().unwrap();

        std::thread::spawn(move || -> anyhow::Result<()> {
            assert_eq!(
                reqwest::blocking::Client::new()
                    .get(format!(
                        "{}:{}/api/v1{SHUTDOWN_URI}",
                        Self::url_base(),
                        port
                    ))
                    .bearer_auth(admin_token)
                    .send()?
                    .status()
                    .as_u16(),
                204
            );

            Ok(())
        })
        .join()
        .map_err(|e| anyhow!("{:?}", e))??;

        Ok(())
    }

    fn url(&self, uri: &str) -> String {
        format!("{}:{}/api/v1{uri}", Self::url_base(), self.port)
    }

    fn url_base() -> String {
        format!("{PROTOCOL}://{HOST}")
    }
}

impl Drop for TestServerImpl {
    fn drop(&mut self) {
        let shutdown_result = if self.process.try_wait().unwrap().is_none() {
            Self::shutdown_server(self)
        } else {
            anyhow::Ok(())
        };

        if shutdown_result.is_err() {
            let _ = self.process.kill();
        }

        let _ = self.process.wait();

        shutdown_result.unwrap();
        Self::remove_dir_if_exists(&self.dir).unwrap();
    }
}