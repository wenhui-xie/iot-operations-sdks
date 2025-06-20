// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Adapter layer for the rumqttc crate

use std::{fmt, fs, time::Duration};
use std::cell::RefCell;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BytesMut, Bytes};
use thiserror::Error;

use crate::connection_settings::MqttConnectionSettings;
use crate::control_packet::{
    AuthProperties, Publish, PublishProperties, QoS, SubscribeProperties, UnsubscribeProperties,
};
use crate::error::{
    AckError, AckErrorKind, ConnectionError, DisconnectError, DisconnectErrorKind, PublishError,
    PublishErrorKind, ReauthError, ReauthErrorKind, SubscribeError, SubscribeErrorKind,
    UnsubscribeError, UnsubscribeErrorKind,
};
use crate::interface::{
    CompletionToken, Event, MqttAck, MqttClient, MqttDisconnect, MqttEventLoop, MqttPubSub, PublishCallback
};
use crate::topic::{TopicFilter, TopicName};
use crate::session::receiver::IncomingPublishDispatcher;
use crate::session::reconnect_policy::ReconnectPolicy;
use crate::session::state::SessionState;

pub type ClientAlias = client::Client<Bytes>;
pub type EventLoopAlias = client::Session<BytesMut>;

impl From<client::Error> for PublishError {
    fn from(err: client::Error) -> Self {
        // NOTE: Technically, the rumqttc ClientError can also include some input validation for
        // publish topics but since there's no way to identify those, we will need to check for them
        // ourselves anyway ahead of invoking rumqttc, thus preventing that case from happening.
        // As such, we can assume that all rumqttc ClientErrors on publish are due to the client
        // being detached from the event loop
        match err {
            _ => {
                PublishError::new(PublishErrorKind::DetachedClient)
            }
        }
    }
}

impl From<client::Error> for SubscribeError {
    fn from(err: client::Error) -> Self {
        // NOTE: Technically, the rumqttc ClientError can also include some input validation for
        // subscribe topics but since there's no way to identify those, we will need to check for them
        // ourselves anyway ahead of invoking rumqttc, thus preventing that case from happening.
        // As such, we can assume that all rumqttc ClientErrors on subscribe are due to the client
        // being detached from the event loop
        match err {
            _ => {
                SubscribeError::new(SubscribeErrorKind::DetachedClient)
            }
        }
    }
}

impl From<client::Error> for UnsubscribeError {
    fn from(err: client::Error) -> Self {
        match err {
            _ => {
                UnsubscribeError::new(UnsubscribeErrorKind::DetachedClient)
            }
        }
    }
}

impl From<client::Error> for AckError {
    fn from(err: client::Error) -> Self {
        match err {
            _ => {
                AckError::new(AckErrorKind::DetachedClient)
            }
        }
    }
}

impl From<client::Error> for DisconnectError {
    fn from(err: client::Error) -> Self {
        match err {
            _ => {
                DisconnectError::new(DisconnectErrorKind::DetachedClient)
            }
        }
    }
}

impl From<client::Error> for ReauthError {
    fn from(err: client::Error) -> Self {
        match err {
            _ => {
                ReauthError::new(ReauthErrorKind::DetachedClient)
            }
        }
    }
}

#[async_trait]
impl MqttPubSub for client::Client<Bytes> {
    // NOTE: Ideally, we would just directly put the result of the MqttPubSub operations in a Box
    // without the intermediate step of calling .wait_async(), but the rumqttc NoticeFuture does
    // not actually implement Future despite the name.

    // NOTE: Validating the topic name here does unfortunately require an additional allocation.
    // This is only true because rumqttc will always reallocate, even if it's being given an owned
    // string.

    // NOTE: It also might be nice to be able to provide the exact reason the topic is invalid here,
    // but the current topic parsing API doesn't have a way to do this without yet another allocation.
    // This may be worth reconsidering in the future, but for now, this is already more information
    // than was previously available.

    async fn publish(
        &self,
        topic: impl Into<String> + Send,
        qos: QoS,
        retain: bool,
        payload: impl Into<Bytes> + Send,
    ) -> Result<CompletionToken, PublishError> {
        let topic = topic.into();
        if !TopicName::is_valid_topic_name(&topic) {
            return Err(PublishError::new(PublishErrorKind::InvalidTopicName));
        }
        let token = self.publish(topic, qos, retain, payload, None).await?;
        Ok(CompletionToken(Box::new(token)))
    }

    async fn publish_with_properties(
        &self,
        topic: impl Into<String> + Send,
        qos: QoS,
        retain: bool,
        payload: impl Into<Bytes> + Send,
        properties: PublishProperties,
    ) -> Result<CompletionToken, PublishError> {
        let topic = topic.into();
        if !TopicName::is_valid_topic_name(&topic) {
            return Err(PublishError::new(PublishErrorKind::InvalidTopicName));
        }
        let token = self
            .publish(topic, qos, retain, payload, Some(properties))
            .await?;
        Ok(CompletionToken(Box::new(token)))
    }

    async fn subscribe(
        &self,
        topic: impl Into<String> + Send,
        qos: QoS,
    ) -> Result<CompletionToken, SubscribeError> {
        let topic = topic.into();
        if !TopicFilter::is_valid_topic_filter(&topic) {
            return Err(SubscribeError::new(SubscribeErrorKind::InvalidTopicFilter));
        }
        let token = self.subscribe(topic, qos, None).await?;
        Ok(CompletionToken(Box::new(token)))
    }

    async fn subscribe_with_properties(
        &self,
        topic: impl Into<String> + Send,
        qos: QoS,
        properties: SubscribeProperties,
    ) -> Result<CompletionToken, SubscribeError> {
        let topic = topic.into();
        if !TopicFilter::is_valid_topic_filter(&topic) {
            return Err(SubscribeError::new(SubscribeErrorKind::InvalidTopicFilter));
        }
        let token = self
            .subscribe(topic, qos, Some(properties))
            .await?;
        Ok(CompletionToken(Box::new(token)))
    }

    async fn unsubscribe(
        &self,
        topic: impl Into<String> + Send,
    ) -> Result<CompletionToken, UnsubscribeError> {
        let topic = topic.into();
        if !TopicFilter::is_valid_topic_filter(&topic) {
            return Err(UnsubscribeError::new(
                UnsubscribeErrorKind::InvalidTopicFilter,
            ));
        }
        let token = self.unsubscribe(topic, None).await?;
        Ok(CompletionToken(Box::new(token)))
    }

    async fn unsubscribe_with_properties(
        &self,
        topic: impl Into<String> + Send,
        properties: UnsubscribeProperties,
    ) -> Result<CompletionToken, UnsubscribeError> {
        let topic = topic.into();
        if !TopicFilter::is_valid_topic_filter(&topic) {
            return Err(UnsubscribeError::new(
                UnsubscribeErrorKind::InvalidTopicFilter,
            ));
        }
        let token = self.unsubscribe(topic, Some(properties)).await?;
        Ok(CompletionToken(Box::new(token)))
    }
}

#[async_trait]
impl MqttAck for client::Client<Bytes> {
    async fn ack(&self, publish: &Publish) -> Result<CompletionToken, AckError> {
        self.ack(publish.pkid, codec::packet::PubAckReasonCode::default()).await?;
        Ok(CompletionToken(Box::new(async { Ok(()) })))
    }
}

#[async_trait]
impl MqttClient for client::Client<Bytes> {
    async fn reauth(&self, _auth_props: AuthProperties) -> Result<(), ReauthError> {
        // FIXME:: Auth is not supported by unified mqtt client.
        Ok(())
    }
}

#[async_trait]
impl MqttDisconnect for client::Client<Bytes> {
    async fn disconnect(&self) -> Result<(), DisconnectError> {
        Ok(self.disconnect(codec::packet::DisconnectReasonCode::default(), None).await?)
    }
}

#[async_trait(?Send)]
impl MqttEventLoop for client::Session<BytesMut> {
    async fn poll(&mut self) -> Result<Event, ConnectionError> {
        let reason_code = self.run().await;
        Err(reason_code)
    }

    fn set_clean_start(&mut self, clean_start: bool) {
        
    }

    fn set_authentication_method(&mut self, authentication_method: Option<String>) {
        
    }

    fn set_authentication_data(&mut self, authentication_data: Option<Bytes>) {
        
    }

    fn set_publish_callback(&mut self, callback: PublishCallback) {
        self.set_publish_callback(Some(callback));
    }

    fn set_connection_callback(&mut self, state: Arc<SessionState>, reconnect_policy: Box<dyn ReconnectPolicy>) {
        let callback = ConnectionCallbackImpl {
            state,
            reconnect_policy,
            prev_reconnect_attempts: RefCell::new(0),
        };
        self.set_connection_callback(Some(Box::new(callback)));
    }
}

/// Client constructors + TLS
/// -------------------------------------------
pub fn client(
    connection_settings: MqttConnectionSettings,
    channel_capacity: usize,
    manual_ack: bool,
    connection_user_properties: Vec<(String, String)>,
) -> Result<(client::Client<Bytes>, client::Session<BytesMut>), MqttAdapterError> {
    // NOTE: channel capacity for AsyncClient must be less than usize::MAX - 1 due to (presumably) a bug.
    // It panics if you set MAX, although MAX - 1 is fine.
    if channel_capacity == usize::MAX {
        return Err(MqttAdapterError::Other(
            "mqtt client does not support channel capacity of usize::MAX".to_string(),
        ));
    }

    let mut mqtt_options_builder: client::options::ConnectionOptionsBuilder<Bytes> = connection_settings.clone().try_into()?;
    for (key, value) in connection_user_properties {
        mqtt_options_builder = mqtt_options_builder.with_user_property(key, value);
    }

    let (mut session,  client) = client::Session::new(connection_settings.hostname, connection_settings.tcp_port, mqtt_options_builder.build(), channel_capacity);
    session.set_manual_ack(manual_ack);

    // Set the TLS connector.
    if connection_settings.use_tls {

        // Create a TLS connector.
        let mut connector = client::network::tokio_tls::NativeTlsConnector::new();
        let ca = if let Some(ca_file) = connection_settings.ca_file {
            let ca = fs::read(ca_file).expect("Failed to read CA certificate");
            Some(ca)
        } else { None };
        let client_cert = if let Some(cert_file) = connection_settings.cert_file {
            let cert = fs::read(cert_file).expect("Failed to read client certificate");
            Some(cert)
        } else { None };
        let client_key = if let Some(key_file) = connection_settings.key_file {
            let key = fs::read(key_file).expect("Failed to read client key");
            Some(key)
        } else { None };
        connector.set_credential(ca, client_cert, client_key);
        session.set_connector(Box::new(connector));
    }

    Ok((client, session))
}

#[async_trait::async_trait(?Send)]
impl<A: MqttAck + Clone + Send + Sync + 'static> client::PublishCallback<Bytes> for IncomingPublishDispatcher<A> {
    async fn publish(&self, publish: codec::packet::Publish<Bytes>) {
        let _ = self.dispatch_publish(&publish);
    }
}

struct ConnectionCallbackImpl {
    state: Arc<SessionState>,
    reconnect_policy: Box<dyn ReconnectPolicy>,
    prev_reconnect_attempts: RefCell<u32>,
}

impl client::ConnectionCallback<Bytes> for ConnectionCallbackImpl {
    fn status(&self, status: &client::ConnectionStatus, options: &mut client::options::ConnectionOptions<Bytes>) -> Option<Duration>
    {
        log::info!("Connection status: {status:?}");
        match status {
            client::ConnectionStatus::Connected => {

                // Update connection state
                self.state.transition_connected();

                // Reset the counter on reconnect attempts
                self.prev_reconnect_attempts.replace(0);

                // Set clean start to false for subsequent connections
                options.set_clean_start(false);

                None
            }
            client::ConnectionStatus::Disconnected(reason) => {
                    self.state.transition_disconnected();

                    // Always log the error itself at error level
                    log::error!("Error: {reason:?}");
                    let mut count = self.prev_reconnect_attempts.borrow_mut();

                    // Defer decision to reconnect policy
                    if let Some(delay) = self
                        .reconnect_policy
                        .next_reconnect_delay(*count, &reason)
                    {
                        *count += 1;
                        Some(delay)
                    } else {
                        None
                    }
            }
            _ => {
                // For other statuses, we don't need to do anything special
                None
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum MqttAdapterError {
    #[error(transparent)]
    ConnectionSettings(#[from] ConnectionSettingsAdapterError),
    #[error("Other adapter error: {0}")]
    Other(String),
}

// TODO: This error story needs improvement once we find out how much of this
// adapter code will stay after TLS dependency changes.
#[derive(Error, Debug)]
#[error("{msg}: {field}")]
pub struct ConnectionSettingsAdapterError {
    msg: String,
    field: ConnectionSettingsField,
    #[source]
    source: Option<Box<dyn std::error::Error>>,
}

// TODO: As above, this will potentially be updated once final TLS implementation takes shape
#[derive(Debug)]
pub enum ConnectionSettingsField {
    // ClientId(String),
    // HostName(String),
    // TcpPort(u16),
    // KeepAlive(Duration),
    SessionExpiry(Duration),
    // ConnectionTimeout(Duration),
    // CleanStart(bool),
    // Username(String),
    // Password(String),
    PasswordFile(String),
    UseTls(bool),
    // CaFile(String),
    // CaRequireRevocationCheck(bool),
    // CertFile(String),
    // KeyFile(String),
    // KeyFilePassword(String),
    SatAuthFile(String),
}

impl fmt::Display for ConnectionSettingsField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionSettingsField::SessionExpiry(v) => write!(f, "Session Expiry: {v:?}"),
            ConnectionSettingsField::PasswordFile(v) => write!(f, "Password File: {v:?}"),
            ConnectionSettingsField::UseTls(v) => write!(f, "Use TLS: {v:?}"),
            ConnectionSettingsField::SatAuthFile(v) => write!(f, "SAT Auth File: {v:?}"),
        }
    }
}

#[derive(Error, Debug)]
#[error("{msg}")]
pub struct TlsError {
    msg: String,
    source: Option<anyhow::Error>,
}

impl TlsError {
    pub fn new(msg: &str) -> Self {
        TlsError {
            msg: msg.to_string(),
            source: None,
        }
    }
}

impl TryFrom<MqttConnectionSettings> for client::options::ConnectionOptionsBuilder<Bytes> {
    type Error = ConnectionSettingsAdapterError;

    fn try_from(value: MqttConnectionSettings) -> Result<Self, Self::Error> {
        // Client ID, Host Name, TCP Port
        let mut mqtt_options =
            client::ConnectionOptionsBuilder::new(value.client_id.clone())
        // Keep Alive
        .with_keep_alive(value.keep_alive.as_secs() as u16)
        // Receive Maximum
        .with_receive_maximum(value.receive_max)
        // Max Packet Size
        // FIXME: check this properties.
        .with_max_packet_size(value.receive_packet_size_max.unwrap_or(u32::MAX))
        // Clean Start
        .with_clean_start(value.clean_start);
        // Session Expiry
        match value.session_expiry.as_secs().try_into() {
            Ok(se) => {
                // validate this is >= 5 seconds otherwise rumqttc will panic
                if se < 5 {
                    return Err(ConnectionSettingsAdapterError {
                        msg: "require > 5 seconds".to_string(),
                        field: ConnectionSettingsField::SessionExpiry(value.session_expiry),
                        source: None,
                    });
                }
                mqtt_options = mqtt_options.with_session_expiry_interval(se);
            }
            Err(e) => {
                return Err(ConnectionSettingsAdapterError {
                    msg: "cannot convert to u32".to_string(),
                    field: ConnectionSettingsField::SessionExpiry(value.session_expiry),
                    source: Some(Box::new(e)),
                });
            }
        };
        // FIXME: Connection Timeout is not supported by unified mqtt client.
        //mqtt_options.set_connection_timeout(value.connection_timeout.as_secs());
        // Username, Password, Password File
        if let Some(username) = value.username {
            let password = {
                if let Some(password_file) = value.password_file {
                    match fs::read_to_string(&password_file) {
                        Ok(password) => password,
                        Err(e) => {
                            return Err(ConnectionSettingsAdapterError {
                                msg: "cannot read password file".to_string(),
                                field: ConnectionSettingsField::PasswordFile(password_file),
                                source: Some(Box::new(e)),
                            });
                        }
                    }
                } else {
                    value.password.unwrap_or_default()
                }
            };
            mqtt_options = mqtt_options.with_username(username).with_password(password)
        }

        // Use TLS, CA File, CA Require Revocation Check, Cert File, Key File, Key File Password
        // FXIME: TLS is set by Session Options, not Connection Options.

        // SAT Auth File
        if let Some(sat_file) = value.sat_file {
            let sat_auth =
                fs::read(sat_file.clone()).map_err(|e| ConnectionSettingsAdapterError {
                    msg: "cannot read sat auth file".to_string(),
                    field: ConnectionSettingsField::SatAuthFile(sat_file),
                    source: Some(Box::new(e)),
                })?;
            mqtt_options = mqtt_options.with_authentication_method("K8S-SAT").with_authentication_data(sat_auth);
        }

        Ok(mqtt_options)
    }
}
