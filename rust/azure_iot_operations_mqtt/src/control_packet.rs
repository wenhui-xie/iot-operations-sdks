// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Structures representing MQTT control packets.

// TODO: Re-implement these instead of just aliasing / add to rumqttc adapter

use bytes::Bytes;

/// Quality of Service
pub type QoS = codec::packet::QoS;

/// PUBLISH packet
pub type Publish = codec::packet::Publish<Bytes>;

/// Properties for a CONNECT packet
pub type ConnectProperties = codec::packet::ConnectProperties<Bytes>;
/// Properties for a PUBLISH packet
pub type PublishProperties = codec::packet::PublishProperties<Bytes>;
/// Properties for a SUBSCRIBE packet
pub type SubscribeProperties = codec::packet::SubscribeProperties<Bytes>;
/// Properties for a UNSUBSCRIBE packet
pub type UnsubscribeProperties = codec::packet::UnsubscribeProperties<Bytes>;
/// Properties for an AUTH packet
pub type AuthProperties = rumqttc::v5::mqttbytes::v5::AuthProperties;
