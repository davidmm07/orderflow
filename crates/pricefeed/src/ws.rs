//! WebSocket endpoint: live index prices for subscribed markets.
//!
//! Protocol (JSON text frames):
//!
//! ```text
//! client: {"op":"subscribe","markets":["BTC-USD","ETH-USD"]}
//! client: {"op":"unsubscribe","markets":["ETH-USD"]}
//! client: {"op":"ping"}
//! server: {"type":"welcome","markets":[...],"max_subscriptions":50,"ping_interval_secs":20}
//! server: {"type":"subscribed","markets":["BTC-USD"]}
//! server: {"type":"price","market":"BTC-USD","price":"81650.5",...}
//! server: {"type":"error","code":"unknown_market","detail":"...","markets":["XYZ-USD"]}
//! server: {"type":"pong"}
//! ```
//!
//! A subscription starts with the current price, then follows changes.
//! Updates for one market always arrive in sequence order; a client that
//! reads slowly gets the newest price, never a backlog.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::{
        State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket, close_code},
    },
    response::{IntoResponse, Response},
};
use orderflow_domain::MarketId;
use tokio_stream::{StreamExt, StreamMap, wrappers::WatchStream};

use crate::{
    dto::{ClientMessage, ServerMessage},
    hub::PriceFrame,
    metrics::Disconnect,
    problem::Problem,
    state::AppState,
};

/// `GET /v1/ws`
pub async fn upgrade(State(state): State<AppState>, upgrade: WebSocketUpgrade) -> Response {
    // The connection budget is taken before the upgrade, so a node at its
    // limit answers 503 and the load balancer tries another one.
    let Ok(permit) = state.connections.clone().try_acquire_owned() else {
        state.metrics.ws_connection_rejected();
        return Problem::service_unavailable(
            "too_many_connections",
            "this node is at its WebSocket connection limit, retry on another one",
        )
        .into_response();
    };
    let limits = state.ws;
    let sessions = state.sessions.clone();
    // Buffers sized for this protocol instead of the library's 128 KiB
    // defaults: clients only send small control messages, and frames are
    // flushed one by one. At 10,000 connections that is the difference
    // between about 1.3 GB and well under 100 MB. The write buffer cap also
    // bounds what a stuck client can hold before its sends start failing.
    upgrade
        .max_message_size(limits.max_message_bytes)
        .max_frame_size(limits.max_message_bytes)
        .read_buffer_size(READ_BUFFER_BYTES)
        .write_buffer_size(0)
        .max_write_buffer_size(MAX_WRITE_BUFFER_BYTES)
        .on_upgrade(move |socket| {
            // Tracked, so shutdown can wait for every session to say goodbye.
            sessions.track_future(async move {
                state.metrics.ws_connection_opened();
                let (reason, subscriptions) = Session::new(state.clone()).run(socket).await;
                state.metrics.ws_connection_closed(reason, subscriptions);
                drop(permit);
            })
        })
        .into_response()
}

/// Read buffer per connection.
const READ_BUFFER_BYTES: usize = 4 * 1024;
/// Frames a connection may have queued but unsent before it is dropped.
const MAX_WRITE_BUFFER_BYTES: usize = 64 * 1024;

type Subscriptions = StreamMap<MarketId, WatchStream<Option<Arc<PriceFrame>>>>;

struct Session {
    state: AppState,
    subscriptions: Subscriptions,
    /// Last `(epoch, sequence)` sent per market, to count skipped updates.
    sent: HashMap<MarketId, (u64, u64)>,
    last_seen: Instant,
    control_messages: u32,
}

/// What the session loop does next.
enum Step {
    Continue,
    Close(Disconnect, Option<CloseFrame>),
    Gone(Disconnect),
}

impl Session {
    fn new(state: AppState) -> Self {
        Self {
            state,
            subscriptions: StreamMap::new(),
            sent: HashMap::new(),
            last_seen: Instant::now(),
            control_messages: 0,
        }
    }

    async fn run(mut self, mut socket: WebSocket) -> (Disconnect, usize) {
        let limits = self.state.ws;
        let welcome = ServerMessage::Welcome {
            markets: self
                .state
                .hub
                .markets()
                .iter()
                .map(ToString::to_string)
                .collect(),
            max_subscriptions: limits.max_subscriptions,
            ping_interval_secs: limits.ping_interval.as_secs(),
        };
        if let Step::Gone(reason) = self.send(&mut socket, welcome.to_json()).await {
            return (reason, 0);
        }

        let mut heartbeat = tokio::time::interval(limits.ping_interval);
        heartbeat.reset();
        let shutdown = self.state.shutdown.clone();
        let step = loop {
            let step = tokio::select! {
                () = shutdown.cancelled() => Step::Close(
                    Disconnect::Shutdown,
                    Some(close(close_code::AWAY, "server shutting down")),
                ),
                _ = heartbeat.tick() => self.heartbeat(&mut socket).await,
                Some((market, frame)) = self.subscriptions.next(), if !self.subscriptions.is_empty() => {
                    match frame {
                        Some(frame) => self.deliver(&mut socket, market, &frame).await,
                        None => Step::Continue,
                    }
                }
                message = socket.recv() => self.receive(&mut socket, message).await,
            };
            if !matches!(step, Step::Continue) {
                break step;
            }
        };

        let subscriptions = self.subscriptions.len();
        match step {
            Step::Close(reason, frame) => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(1),
                    socket.send(Message::Close(frame)),
                )
                .await;
                (reason, subscriptions)
            }
            Step::Gone(reason) => (reason, subscriptions),
            Step::Continue => (Disconnect::Error, subscriptions),
        }
    }

    /// Sends a ping, or gives up on a client that went silent.
    async fn heartbeat(&mut self, socket: &mut WebSocket) -> Step {
        if self.last_seen.elapsed() > self.state.ws.idle_timeout {
            return Step::Gone(Disconnect::Idle);
        }
        // The control message budget refills once per heartbeat.
        self.control_messages = 0;
        self.send_message(socket, Message::Ping(Default::default()))
            .await
    }

    async fn deliver(
        &mut self,
        socket: &mut WebSocket,
        market: MarketId,
        frame: &PriceFrame,
    ) -> Step {
        let (epoch, sequence) = (frame.update.epoch, frame.update.sequence);
        if let Some((last_epoch, last_sequence)) = self.sent.insert(market, (epoch, sequence))
            && last_epoch == epoch
            && sequence > last_sequence + 1
        {
            self.state
                .metrics
                .ws_conflated(sequence - last_sequence - 1);
        }
        let step = self
            .send_message(socket, Message::Text(frame.text.clone()))
            .await;
        if matches!(step, Step::Continue) {
            self.state.metrics.ws_sent(1);
        }
        step
    }

    async fn receive(
        &mut self,
        socket: &mut WebSocket,
        message: Option<Result<Message, axum::Error>>,
    ) -> Step {
        self.last_seen = Instant::now();
        let text = match message {
            None => return Step::Gone(Disconnect::Client),
            Some(Err(_)) => return Step::Gone(Disconnect::Error),
            Some(Ok(Message::Close(_))) => return Step::Gone(Disconnect::Client),
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => return Step::Continue,
            Some(Ok(Message::Binary(_))) => {
                return self
                    .reply(
                        socket,
                        ServerMessage::error(
                            "invalid_message",
                            "only text frames are accepted",
                            Vec::new(),
                        ),
                    )
                    .await;
            }
            Some(Ok(Message::Text(text))) => text,
        };

        self.control_messages += 1;
        if self.control_messages > self.state.ws.max_control_messages {
            return Step::Close(
                Disconnect::Policy,
                Some(close(close_code::POLICY, "too many messages")),
            );
        }
        match serde_json::from_str::<ClientMessage>(text.as_str()) {
            Ok(ClientMessage::Subscribe { markets }) => self.subscribe(socket, &markets).await,
            Ok(ClientMessage::Unsubscribe { markets }) => self.unsubscribe(socket, &markets).await,
            Ok(ClientMessage::Ping) => self.reply(socket, ServerMessage::Pong).await,
            Err(_) => {
                let detail =
                    r#"expected {"op":"subscribe"|"unsubscribe","markets":[...]} or {"op":"ping"}"#;
                self.reply(
                    socket,
                    ServerMessage::error("invalid_message", detail, Vec::new()),
                )
                .await
            }
        }
    }

    async fn subscribe(&mut self, socket: &mut WebSocket, requested: &[String]) -> Step {
        let max = self.state.ws.max_subscriptions;
        if requested.len() > max {
            let detail = format!("at most {max} markets per connection");
            return self
                .reply(
                    socket,
                    ServerMessage::error("too_many_subscriptions", detail, Vec::new()),
                )
                .await;
        }
        let markets = match self.validate(requested) {
            Ok(markets) => markets,
            Err(error) => return self.reply(socket, *error).await,
        };
        let new: Vec<MarketId> = markets
            .into_iter()
            .filter(|market| !self.subscriptions.contains_key(market))
            .collect();
        if self.subscriptions.len() + new.len() > max {
            let detail = format!("at most {max} markets per connection");
            return self
                .reply(
                    socket,
                    ServerMessage::error("too_many_subscriptions", detail, Vec::new()),
                )
                .await;
        }
        for market in new {
            if let Some(receiver) = self.state.hub.subscribe(&market) {
                // WatchStream yields the current price first, so a new
                // subscriber gets a snapshot without a separate request.
                self.subscriptions
                    .insert(market, WatchStream::new(receiver));
                self.state.metrics.ws_subscriptions_changed(1);
            }
        }
        self.confirm(socket).await
    }

    async fn unsubscribe(&mut self, socket: &mut WebSocket, requested: &[String]) -> Step {
        let markets = match self.validate(requested) {
            Ok(markets) => markets,
            Err(error) => return self.reply(socket, *error).await,
        };
        for market in markets {
            if self.subscriptions.remove(&market).is_some() {
                self.sent.remove(&market);
                self.state.metrics.ws_subscriptions_changed(-1);
            }
        }
        self.confirm(socket).await
    }

    /// Parses market ids. Malformed ids are never echoed back: only ids
    /// that passed validation appear in error messages.
    fn validate(&self, requested: &[String]) -> Result<Vec<MarketId>, Box<ServerMessage>> {
        let mut markets = Vec::with_capacity(requested.len());
        let mut unknown = Vec::new();
        for raw in requested {
            let Ok(market) = MarketId::parse(raw) else {
                return Err(Box::new(ServerMessage::error(
                    "invalid_market",
                    "market ids look like BTC-USD",
                    Vec::new(),
                )));
            };
            if self.state.hub.contains(&market) {
                markets.push(market);
            } else {
                unknown.push(market.to_string());
            }
        }
        if unknown.is_empty() {
            Ok(markets)
        } else {
            Err(Box::new(ServerMessage::error(
                "unknown_market",
                "these markets are not listed",
                unknown,
            )))
        }
    }

    async fn confirm(&mut self, socket: &mut WebSocket) -> Step {
        let mut markets: Vec<String> = self.subscriptions.keys().map(ToString::to_string).collect();
        markets.sort();
        self.reply(socket, ServerMessage::Subscribed { markets })
            .await
    }

    async fn reply(&mut self, socket: &mut WebSocket, message: ServerMessage) -> Step {
        self.send(socket, message.to_json()).await
    }

    async fn send(&mut self, socket: &mut WebSocket, text: String) -> Step {
        self.send_message(socket, Message::Text(text.into())).await
    }

    /// Sends one frame within the send timeout. A client whose socket stays
    /// full that long is too slow to serve and is dropped, which frees its
    /// connection slot for someone who can keep up.
    async fn send_message(&mut self, socket: &mut WebSocket, message: Message) -> Step {
        match tokio::time::timeout(self.state.ws.send_timeout, socket.send(message)).await {
            Ok(Ok(())) => Step::Continue,
            Ok(Err(_)) => Step::Gone(Disconnect::Error),
            Err(_) => Step::Gone(Disconnect::SlowConsumer),
        }
    }
}

fn close(code: u16, reason: &'static str) -> CloseFrame {
    CloseFrame {
        code,
        reason: reason.into(),
    }
}
