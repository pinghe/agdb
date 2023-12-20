use crate::ChangePassword;
use crate::TestServer;
use crate::User;
use crate::NO_TOKEN;
use crate::USER_CHANGE_PASSWORD_URI;
use crate::USER_LOGIN_URI;

#[tokio::test]
async fn change_password() -> anyhow::Result<()> {
    let server = TestServer::new().await?;
    let user = server.init_user().await?;
    let change = ChangePassword {
        name: &user.name,
        password: &user.name,
        new_password: "password456",
    };
    let user = User {
        name: &user.name,
        password: "password456",
    };
    assert_eq!(
        server
            .post(USER_CHANGE_PASSWORD_URI, &change, NO_TOKEN)
            .await?
            .0,
        201
    );
    assert_eq!(server.post(USER_LOGIN_URI, &user, NO_TOKEN).await?.0, 200);

    Ok(())
}

#[tokio::test]
async fn invalid_credentials() -> anyhow::Result<()> {
    let server = TestServer::new().await?;
    let user = server.init_user().await?;
    let change = ChangePassword {
        name: &user.name,
        password: "bad_password",
        new_password: "password456",
    };
    assert_eq!(
        server
            .post(USER_CHANGE_PASSWORD_URI, &change, NO_TOKEN)
            .await?
            .0,
        401
    );

    Ok(())
}

#[tokio::test]
async fn password_too_short() -> anyhow::Result<()> {
    let server = TestServer::new().await?;
    let user = server.init_user().await?;
    let change = ChangePassword {
        name: &user.name,
        password: &user.name,
        new_password: "pswd",
    };
    assert_eq!(
        server
            .post(USER_CHANGE_PASSWORD_URI, &change, NO_TOKEN)
            .await?
            .0,
        461
    );

    Ok(())
}

#[tokio::test]
async fn user_not_found() -> anyhow::Result<()> {
    let server = TestServer::new().await?;
    let change = ChangePassword {
        name: "user_not_found",
        password: "password123",
        new_password: "password456",
    };
    assert_eq!(
        server
            .post(USER_CHANGE_PASSWORD_URI, &change, NO_TOKEN)
            .await?
            .0,
        464
    );

    Ok(())
}