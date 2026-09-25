use super::info::{InfoBridge, MAX_PENDING, PostRequest, PostResponse};
use crate::{
    listeners::order_book::{InternalMessage, L2SnapshotParams, OrderBookListener, TimedSnapshots, hl_listen},
    order_book::{Coin, Snapshot},
    prelude::*,
    telemetry::{self, SOCKET_TELEMETRY, SocketTelemetry},
    types::{
        L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{ClientMessage, DEFAULT_LEVELS, ServerResponse, Subscription, SubscriptionManager},
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{FutureExt, SinkExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

pub async fn run_websocket_server(
    address: &str,
    ignore_spot: bool,
    compression_level: u32,
    config: crate::ServerConfig,
) -> Result<()> {
    config.validate()?;
    let info_bridge = InfoBridge::new(config.info_url.clone())?;
    let wallet = crate::wallet::WalletHub::new(&config)?;
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(100);

    // Central task: listen to messages and forward them for distribution
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        let mut book = OrderBookListener::new(internal_message_tx, config.clone());
        book.wallet = Some(wallet.clone());
        book
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen(listener, config).await {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let health_listener = listener.clone();
    let diagnostics_listener = listener.clone();
    let app = Router::new()
        .route("/version", get(|| async { axum::Json(telemetry::version()) }))
        .route("/capabilities", get(|| async { axum::Json(telemetry::capabilities()) }))
        .route(
            "/diagnostics",
            get(move || {
                let listener = diagnostics_listener.clone();
                async move { axum::Json(listener.lock().await.diagnostics()) }
            }),
        )
        .route(
            "/health",
            get(move || {
                let listener = health_listener.clone();
                async move {
                    let book = listener.lock().await;
                    let code = if book.is_ready() {
                        axum::http::StatusCode::OK
                    } else {
                        axum::http::StatusCode::SERVICE_UNAVAILABLE
                    };
                    (code, axum::Json(book.status.clone()))
                }
            }),
        )
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                async move |ws_upgrade| {
                    ws_handler(
                        ws_upgrade,
                        internal_message_tx.clone(),
                        listener.clone(),
                        ignore_spot,
                        websocket_opts,
                        info_bridge.clone(),
                    )
                }
            }),
        );

    let listener = TcpListener::bind(address).await?;
    info!("WebSocket server running at ws://{address} build={}", telemetry::version());

    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    websocket_opts: yawc::Options,
    info_bridge: InfoBridge,
) -> impl IntoResponse {
    let (resp, fut) = match incoming.upgrade(websocket_opts) {
        Ok(upgrade) => upgrade,
        Err(err) => return (axum::http::StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    };
    tokio::spawn(async move {
        let ws = match fut.await {
            Ok(ok) => ok,
            Err(err) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
        };

        handle_socket(ws, internal_message_tx, listener, ignore_spot, info_bridge).await
    });

    resp.into_response()
}

async fn handle_socket(
    socket: WebSocket,
    tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    info_bridge: InfoBridge,
) {
    let metrics = listener.lock().await.metrics.clone();
    SOCKET_TELEMETRY
        .scope(
            SocketTelemetry { metrics, trace: std::cell::RefCell::new(None) },
            handle_socket_inner(socket, tx, listener, ignore_spot, info_bridge),
        )
        .await;
}

async fn handle_socket_inner(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    _ignore_spot: bool,
    info_bridge: InfoBridge,
) {
    let Some(wallet) = listener.lock().await.wallet.clone() else { return };
    let mut wallets = super::wallet_socket::WalletSession::new(wallet);
    let mut wallet_tick = tokio::time::interval(wallets.hub.event_interval);
    wallet_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut info_jobs = FuturesUnordered::<BoxFuture<'static, PostResponse>>::new();
    let mut internal_message_rx = internal_message_tx.subscribe();
    let mut manager = SubscriptionManager::default();
    let mut demand =
        L2Demand { subscriptions: HashSet::new(), registry: listener.lock().await.l2_subscriptions.clone() };
    let mut book_heights = HashMap::<Subscription, u64>::new();
    send_current_status(&mut socket, &listener).await;
    loop {
        select! {
            _ = wallet_tick.tick(), if wallets.polls() => { wallets.schedule(info_bridge.clone()); }
            _ = wallets.signal.changed(), if wallets.active() => {
                telemetry::dispatch(None);
                for message in wallets.updates() { send_socket_value(&mut socket, message).await; }
                wallets.schedule(info_bridge.clone());
            }
            Some(result) = wallets.jobs.next(), if !wallets.jobs.is_empty() => {
                telemetry::dispatch(None);
                for message in wallets.complete(result) { send_socket_value(&mut socket, message).await; }
            }
            Some(response) = info_jobs.next(), if !info_jobs.is_empty() => {
                telemetry::dispatch(None);
                send_socket_message(&mut socket, ServerResponse::Post(response)).await;
            }
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        telemetry::dispatch(msg.trace());
                        match msg.as_ref() {
                            InternalMessage::Status(status) => {
                                if status.state == crate::listeners::order_book::Health::Ready
                                    && listener.lock().await.status.generation != status.generation { continue; }
                                send_socket_message(&mut socket, ServerResponse::Status(status.clone())).await;
                            },
                            InternalMessage::Reset { generation } => {
                                if listener.lock().await.status.generation != *generation { continue; }
                                refresh_books(&mut socket, &manager, &listener, &mut book_heights).await;
                            },
                            InternalMessage::Snapshot{ generation, height, l2_snapshots, time, .. } => {
                                { let book = listener.lock().await;
                                  if !book.is_ready() || book.status.generation != *generation { continue; } }


                                for sub in manager.subscriptions() {
                                    if book_heights.get(sub).is_some_and(|h| *h >= *height) { continue; }
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots.as_ref(), *time).await;
                                    if matches!(sub, Subscription::L2Book { .. }) { book_heights.insert(sub.clone(), *height); }
                                }
                            },
                            InternalMessage::Fills{ batch, .. } => {
                                if !manager.subscriptions().iter().any(|s| matches!(s, Subscription::Trades { .. })) { continue; }
                                let trade_start = std::time::Instant::now();
                                let mut trades = coin_to_trades(batch);
                                if let Some(metrics) = telemetry::socket_metrics() { metrics.elapsed("trade_reconstruct_us", trade_start); }
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_trades(&mut socket, sub, &mut trades).await;
                                }
                            },
                            InternalMessage::L4BookUpdates{ generation, diff_batch, status_batch, .. } => {
                                { let book = listener.lock().await;
                                  if !book.is_ready() || book.status.generation != *generation { continue; } }
                                if !manager.subscriptions().iter().any(|s| matches!(s, Subscription::L4Book { .. })) { continue; }

                                let mut book_updates = coin_to_book_updates(diff_batch, status_batch);
                                for sub in manager.subscriptions() {
                                    if book_heights.get(sub).is_some_and(|h| *h >= diff_batch.block_number()) { continue; }
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await;
                                    if matches!(sub, Subscription::L4Book { .. }) { book_heights.insert(sub.clone(), diff_batch.block_number()); }
                                }
                            },
                        }

                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        telemetry::dispatch(None);
                        if let Some(metrics) = telemetry::socket_metrics() { metrics.observe("client_lagged_messages", n as f64); }
                        internal_message_rx = internal_message_tx.subscribe();
                        send_socket_message(&mut socket, ServerResponse::Error(format!("Client fell behind by {n} messages; book snapshots reset; trades may have gaps"))).await;
                        send_current_status(&mut socket, &listener).await;
                        refresh_books(&mut socket, &manager, &listener, &mut book_heights).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }

            msg = socket.next() => {
                telemetry::dispatch(None);
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return;
                                }
                            };

                            if text.len() > 64 * 1024 {
                                send_socket_message(&mut socket, ServerResponse::Error("Request exceeds 64 KiB limit".into())).await;
                                continue;
                            }
                            if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
                                if super::wallet_socket::is_wallet_request(&value) {
                                    for message in wallets.request(value) { send_socket_value(&mut socket, message).await; }
                                    wallets.schedule(info_bridge.clone());
                                    continue;
                                }
                                if value["method"] == "post" {
                                    match serde_json::from_value::<PostRequest>(value) {
                                        Ok(post) if info_jobs.len() < MAX_PENDING => {
                                            let bridge = info_bridge.clone();
                                            let book_listener = listener.clone();
                                            info_jobs.push(async move {
                                                let id = post.id;
                                                tokio::time::timeout(std::time::Duration::from_secs(2), execute_info_post(post, bridge, book_listener))
                                                    .await.unwrap_or_else(|_| PostResponse::error(id, "504: Info request timed out"))
                                            }.boxed());
                                        }
                                        Ok(post) => send_socket_message(&mut socket, ServerResponse::Post(PostResponse::error(post.id, "429: maximum four pending Info requests per connection"))).await,
                                        Err(_) => send_socket_message(&mut socket, ServerResponse::Error("Invalid post request: id must be an unsigned integer and request is required".into())).await,
                                    }
                                    continue;
                                }
                            }
                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                let universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
                                receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone(), &mut book_heights).await;
                                demand.update(&manager);
                            }
                            else {
                                let msg = ServerResponse::Error(unsupported_request_message(text));
                                send_socket_message(&mut socket, msg).await;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return;
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return;
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    book_heights: &mut HashMap<Subscription, u64>,
) {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
    };
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    let mut universe = universe.clone();
    let ready = {
        let book = listener.lock().await;
        if !book.is_ready() {
            let coin = match &subscription {
                Subscription::L2Book { coin, .. } | Subscription::L4Book { coin } | Subscription::Trades { coin } => {
                    coin
                }
            };
            if book.accepts(coin) {
                universe.insert(coin.clone());
            }
        }
        book.is_ready()
    };
    if !matches!(client_message, ClientMessage::Unsubscribe { .. }) && !subscription.validate(&universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await;
        return;
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
    };
    if success {
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = if ready { subscription.handle_immediate_snapshot(listener).await } else { Ok(None) };
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await;
                    return;
                }
            }
        } else {
            None
        };
        let accepted_sub = match &client_message {
            ClientMessage::Subscribe { subscription } | ClientMessage::Unsubscribe { subscription } => {
                subscription.clone()
            }
        };
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await;
        if let Some((snapshot_msg, height)) = snapshot_msg {
            book_heights.insert(accepted_sub, height);
            send_socket_message(socket, snapshot_msg).await;
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await;
    }
}

async fn execute_info_post(
    post: PostRequest,
    bridge: InfoBridge,
    listener: Arc<Mutex<OrderBookListener>>,
) -> PostResponse {
    if post.request.to_string().len() > 64 * 1024 {
        return PostResponse::error(post.id, "413: Info payload exceeds 64 KiB limit");
    }
    if post.request["type"] == "info" && post.request["payload"]["type"] == "orderStatus" {
        let _permit = match bridge.acquire() {
            Ok(p) => p,
            Err(e) => return PostResponse::error(post.id, e),
        };
        let hub = listener.lock().await.wallet.clone();
        return match hub {
            Some(hub) => match hub.order_status(post.request["payload"].clone()).await {
                Ok(data) => PostResponse {
                    id: post.id,
                    response: serde_json::json!({"type":"info","payload":{"type":"orderStatus","data":data}}),
                },
                Err(e) => PostResponse::error(post.id, e),
            },
            None => PostResponse::error(post.id, "LOCAL_HISTORY_UNAVAILABLE: wallet history disabled"),
        };
    }
    if post.request["type"] == "info" && post.request["payload"]["type"] == "localWalletHistory" {
        let _permit = match bridge.acquire() {
            Ok(p) => p,
            Err(e) => return PostResponse::error(post.id, e),
        };
        let hub = listener.lock().await.wallet.clone();
        return match hub {
            Some(hub) => match hub.history(post.request["payload"].clone()).await {
                Ok(data) => PostResponse {
                    id: post.id,
                    response: serde_json::json!({"type":"info","payload":{"type":"localWalletHistory","data":data}}),
                },
                Err(e) => PostResponse::error(post.id, e),
            },
            None => PostResponse::error(post.id, "wallet history disabled"),
        };
    }
    if post.request["type"] != "info" || post.request["payload"]["type"] != "l2Book" {
        return bridge.execute(post).await;
    }
    let _permit = match bridge.acquire() {
        Ok(permit) => permit,
        Err(error) => return PostResponse::error(post.id, error),
    };
    let Ok(subscription @ Subscription::L2Book { .. }) =
        serde_json::from_value::<Subscription>(post.request["payload"].clone())
    else {
        return PostResponse::error(post.id, "400: invalid l2Book Info payload");
    };
    {
        let book = listener.lock().await;
        if !book.is_ready() {
            return PostResponse::error(post.id, "503: local book is not ready");
        }
        let universe = book.universe().into_iter().map(|c| c.value()).collect();
        if !subscription.validate(&universe) {
            return PostResponse::error(post.id, "400: unsupported market or L2 parameters");
        }
    }
    match subscription.handle_immediate_snapshot(listener).await {
        Ok(Some((ServerResponse::L2Book(book), _))) => PostResponse {
            id: post.id,
            response: serde_json::json!({"type":"info","payload":{"type":"l2Book","data":book}}),
        },
        _ => PostResponse::error(post.id, "503: local book is not ready"),
    }
}

fn unsupported_request_message(text: &str) -> String {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        if value["method"] == "post" {
            return "Invalid WebSocket post; expected id and request with type info. See /capabilities.".into();
        }
        if let Some(kind) = value["subscription"]["type"].as_str() {
            if ["orderUpdates", "userFills", "openOrders"].contains(&kind) {
                return format!(
                    "Subscription {kind} is not implemented by this server. Supported: l2Book, trades, l4Book. See /capabilities."
                );
            }
        }
    }
    "Invalid websocket request. Expected subscribe/unsubscribe with l2Book, trades, or l4Book; see /capabilities."
        .into()
}

// Clone under a separate statement: a temporary guard in an awaited send's
// arguments otherwise stays alive until the peer accepts the write.
async fn send_current_status<S>(socket: &mut S, listener: &Arc<Mutex<OrderBookListener>>)
where
    S: futures_util::Sink<FrameView> + Unpin,
    S::Error: std::fmt::Display,
{
    let status = listener.lock().await.status.clone();
    send_socket_message(socket, ServerResponse::Status(status)).await;
}

async fn send_socket_message<S>(socket: &mut S, msg: ServerResponse)
where
    S: futures_util::Sink<FrameView> + Unpin,
    S::Error: std::fmt::Display,
{
    send_socket_value(socket, msg).await;
}

async fn send_socket_value<S>(socket: &mut S, msg: impl serde::Serialize)
where
    S: futures_util::Sink<FrameView> + Unpin,
    S::Error: std::fmt::Display,
{
    let metrics = telemetry::socket_metrics();
    let trace = telemetry::socket_trace();
    let serialize_start = std::time::Instant::now();
    let msg = serde_json::to_string(&msg);
    let serialization_us = serialize_start.elapsed().as_secs_f64() * 1e6;
    if let Some(metrics) = &metrics {
        metrics.observe("ws_serialize_us", serialization_us);
    }
    match msg {
        Ok(msg) => {
            let send_start = std::time::Instant::now();
            let send_begin_us = telemetry::unix_us();
            let sent = socket.send(FrameView::text(msg)).await;
            let send_end_us = telemetry::unix_us();
            if let Some(metrics) = &metrics {
                metrics.elapsed("ws_socket_send_us", send_start);
                if let Some(t) = trace {
                    metrics.elapsed("read_to_socket_send_complete_us", t.first_read);
                    metrics.observe("event_age_at_send_us", (send_begin_us - t.block_us) as f64);
                }
            }
            if let Some(t) = trace {
                // Opt-in via RUST_LOG=info,latency=debug; sample one height in 100.
                if t.height % 100 == 0 {
                    log::debug!(target: "latency", "height={} block_us={} node_local_us={} file_read_us={} apply_done_us={} publish_us={} serialize_us={serialization_us:.0} send_begin_us={send_begin_us} send_end_us={send_end_us}",
                        t.height,t.block_us,t.node_local_us,t.read_us,t.applied_us,t.published_us);
                }
            }
            if let Err(err) = sent {
                error!("Failed to send: {err}");
            }
        }
        Err(err) => {
            error!("Server response serialization error: {err}");
        }
    }
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshot: &HashMap<Coin, HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>,
    time: u64,
) {
    if let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription {
        let snapshot = snapshot.get(&Coin::new(coin));
        if let Some(snapshot) =
            snapshot.and_then(|snapshot| snapshot.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
        {
            let n_levels = n_levels.unwrap_or(DEFAULT_LEVELS);
            let snapshot = snapshot.truncate(n_levels);
            let snapshot = snapshot.export_inner_snapshot();
            let l2_book = L2Book::from_l2_snapshot(coin.clone(), snapshot, time);
            let msg = ServerResponse::L2Book(l2_book);
            send_socket_message(socket, msg).await;
        } else {
            error!("Coin {coin} not found");
        }
    }
}

fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let mut groups = HashMap::<_, Vec<_>>::new();
    let mut identities = Vec::new();
    for fill in batch.clone().events() {
        let key = (fill.1.coin.clone(), fill.1.tid);
        if !groups.contains_key(&key) {
            identities.push(key.clone());
        }
        groups.entry(key).or_default().push(fill);
    }
    let mut trades = HashMap::new();
    let mut skipped = 0;
    for key in identities {
        let Some(fills) = groups.remove(&key) else { continue };
        if fills.len() != 2 || fills[0].1.side == fills[1].1.side {
            skipped += 1;
            log::debug!("incomplete trade pair coin={} tid={} fills={}", key.0, key.1, fills.len());
            continue;
        }
        match Trade::from_fills(fills.into_iter().map(|f| (f.1.side, f)).collect()) {
            Ok(trade) => trades.entry(trade.coin.clone()).or_insert_with(Vec::new).push(trade),
            Err(err) => {
                skipped += 1;
                log::debug!("invalid trade pair {key:?}: {err}");
            }
        }
    }
    if skipped > 0 {
        log::warn!("block={} incomplete_or_malformed_trade_pairs={skipped}", batch.block_number());
    }
    trades
}

fn coin_to_book_updates(
    diff_batch: &Batch<NodeDataOrderDiff>,
    status_batch: &Batch<NodeDataOrderStatus>,
) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let statuses = status_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            send_socket_message(socket, msg).await;
        }
    }
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            let msg = ServerResponse::Trades(trades);
            send_socket_message(socket, msg).await;
        }
    }
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<(ServerResponse, u64)>> {
        if let Self::L2Book { coin, n_sig_figs, n_levels, mantissa } = self {
            let mut book = listener.lock().await;
            if let Some((time, snapshots)) = book.current_l2(self) {
                if let Some(snapshot) = snapshots
                    .as_ref()
                    .get(&Coin::new(coin))
                    .and_then(|m| m.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
                {
                    return Ok(Some((
                        ServerResponse::L2Book(L2Book::from_l2_snapshot(
                            coin.clone(),
                            snapshot.truncate(n_levels.unwrap_or(DEFAULT_LEVELS)).export_inner_snapshot(),
                            time,
                        )),
                        book.status.height.unwrap_or_default(),
                    )));
                }
            }
            return Err("book is not ready".into());
        }
        if let Self::L4Book { coin } = self {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                let snapshot =
                    snapshot.value().into_iter().filter(|(c, _)| *c == Coin::new(coin)).collect::<Vec<_>>().pop();
                if let Some((coin, snapshot)) = snapshot {
                    let snapshot =
                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some((
                        ServerResponse::L4Book(L4Book::Snapshot { coin: coin.value(), time, height, levels: snapshot }),
                        height,
                    )));
                }
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

async fn refresh_books(
    socket: &mut WebSocket,
    manager: &SubscriptionManager,
    listener: &Arc<Mutex<OrderBookListener>>,
    heights: &mut HashMap<Subscription, u64>,
) {
    for sub in manager.subscriptions() {
        if let Ok(Some((message, height))) = sub.handle_immediate_snapshot(listener.clone()).await {
            heights.insert(sub.clone(), height);
            send_socket_message(socket, message).await;
        }
    }
}

#[cfg(test)]
mod trade_tests {
    use super::*;
    use serde_json::json;
    fn fill(side: &str, tid: u64, coin: &str) -> serde_json::Value {
        json!(["0x0000000000000000000000000000000000000001", {
            "coin":coin,"px":"100","sz":"1","side":side,"time":1000,
            "startPosition":"0","dir":"Open Long","closedPnl":"0","hash":"hash",
            "oid":tid,"crossed":side == "B","fee":"0","tid":tid,"feeToken":"USDC"
        }])
    }
    fn trades(events: Vec<serde_json::Value>) -> HashMap<String, Vec<Trade>> {
        let batch = serde_json::from_value(json!({"local_time":"2026-01-01T00:00:00",
            "block_time":"2026-01-01T00:00:00","block_number":1,"events":events}))
        .unwrap();
        coin_to_trades(&batch)
    }
    #[test]
    fn valid_ask_bid() {
        assert_eq!(trades(vec![fill("A", 1, "BTC"), fill("B", 1, "BTC")])["BTC"].len(), 1);
    }
    #[test]
    fn same_side_is_skipped() {
        assert!(trades(vec![fill("A", 1, "BTC"), fill("A", 1, "BTC")]).is_empty());
    }
    #[test]
    fn incomplete_pair_is_skipped() {
        assert!(trades(vec![fill("B", 1, "BTC")]).is_empty());
    }
    #[test]
    fn interleaved_trades_and_coin_scoping() {
        let result = trades(vec![
            fill("A", 1, "BTC"),
            fill("B", 2, "BTC"),
            fill("A", 1, "HYPE"),
            fill("B", 1, "BTC"),
            fill("A", 2, "BTC"),
            fill("B", 1, "HYPE"),
        ]);
        assert_eq!(result["BTC"].len(), 2);
        assert_eq!(result["HYPE"].len(), 1);
        let values = serde_json::to_value(&result["BTC"]).unwrap();
        assert_eq!(values[0]["tid"], 1);
        assert_eq!(values[1]["tid"], 2);
    }
    #[test]
    fn inconsistent_pair_and_duplicate_are_skipped() {
        let mut bad = fill("B", 1, "BTC");
        bad[1]["px"] = json!("101");
        assert!(trades(vec![fill("A", 1, "BTC"), bad]).is_empty());
        assert!(trades(vec![fill("A", 1, "BTC"), fill("B", 1, "BTC"), fill("B", 1, "BTC")]).is_empty());
    }
}

// Demand is removed even when a socket task is cancelled or the peer closes unexpectedly.
struct L2Demand {
    subscriptions: HashSet<Subscription>,
    registry: Arc<std::sync::Mutex<HashMap<Subscription, usize>>>,
}
impl L2Demand {
    fn update(&mut self, manager: &SubscriptionManager) {
        self.replace(
            manager.subscriptions().iter().filter(|s| matches!(s, Subscription::L2Book { .. })).cloned().collect(),
        );
    }
    fn replace(&mut self, next: HashSet<Subscription>) {
        let mut registry = self.registry.lock().unwrap_or_else(|e| e.into_inner());
        for sub in self.subscriptions.difference(&next) {
            if let Some(count) = registry.get_mut(sub) {
                *count -= 1;
                if *count == 0 {
                    registry.remove(sub);
                }
            }
        }
        for sub in next.difference(&self.subscriptions) {
            *registry.entry(sub.clone()).or_default() += 1;
        }
        self.subscriptions = next;
    }
}
impl Drop for L2Demand {
    fn drop(&mut self) {
        self.replace(HashSet::new());
    }
}

#[cfg(test)]
mod demand_tests {
    use super::*;
    #[test]
    fn demand_survives_other_clients_unsubscribe_and_cleans_up_on_drop() {
        let registry = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let sub = Subscription::L2Book { coin: "BTC".into(), n_sig_figs: None, n_levels: Some(5), mantissa: None };
        let mut manager = SubscriptionManager::default();
        manager.subscribe(sub.clone());
        let mut first = L2Demand { subscriptions: HashSet::new(), registry: registry.clone() };
        let mut second = L2Demand { subscriptions: HashSet::new(), registry: registry.clone() };
        first.update(&manager);
        first.update(&manager); // Duplicate update must not double-count demand.
        second.update(&manager);
        assert_eq!(registry.lock().unwrap()[&sub], 2);
        manager.unsubscribe(sub.clone());
        first.update(&manager);
        assert_eq!(registry.lock().unwrap()[&sub], 1);
        drop(first);
        assert_eq!(registry.lock().unwrap()[&sub], 1);
        drop(second);
        assert!(registry.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod backpressure_tests {
    use super::*;

    #[tokio::test]
    async fn blocked_status_writer_does_not_hold_book_lock() {
        let (tx, _) = channel(100);
        let listener = Arc::new(Mutex::new(OrderBookListener::new(tx, crate::ServerConfig::default())));
        // This sink accepts a frame but never completes its write. Polling once
        // deterministically reaches backpressure, without filling OS buffers.
        let mut sink = Box::pin(futures_util::sink::unfold((), |(), _: FrameView| async {
            futures_util::future::pending::<std::result::Result<(), io::Error>>().await
        }));
        let mut send = Box::pin(send_current_status(&mut sink, &listener));
        assert!(send.as_mut().now_or_never().is_none());
        let book = listener.try_lock().expect("slow client must not block health, diagnostics or reconstruction");
        drop(book.diagnostics());
        drop(book);
        drop(send);
        assert!(listener.try_lock().is_ok());
    }

    #[tokio::test]
    async fn inline_guard_reproduces_previous_global_stall() {
        let (tx, _) = channel(100);
        let listener = Arc::new(Mutex::new(OrderBookListener::new(tx, crate::ServerConfig::default())));
        let mut sink = Box::pin(futures_util::sink::unfold((), |(), _: FrameView| async {
            futures_util::future::pending::<std::result::Result<(), io::Error>>().await
        }));
        let mut old_send = Box::pin(async {
            send_socket_message(&mut sink, ServerResponse::Status(listener.lock().await.status.clone())).await;
        });
        assert!(old_send.as_mut().now_or_never().is_none());
        assert!(listener.try_lock().is_err(), "control must reproduce old lock lifetime");
        drop(old_send);
        assert!(listener.try_lock().is_ok());
    }
}
