use std::{
    io::Write,
    net::SocketAddr,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::Duration,
};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use axum::{
    body::Body,
    extract::{Path, State, WebSocketUpgrade},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Router,
};
use bytes::Bytes;
use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::sync::broadcast;
use tower_http::services::ServeDir;

use crate::{redis_camera, test_camera};

#[derive(Clone)]
pub struct AppState {
    pub hash: String,
    pub broadcaster: broadcast::Sender<Bytes>,
    pub ui_template_path: PathBuf,
    pub port: u16,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub uri: String,
    pub ffmpeg: String,
    pub quality: u8,
    pub size: (u32, u32),
    pub vflip: bool,
    pub hash: String,
    pub in_redis_channel: String,
    pub debug: bool,
}

pub async fn run(config: ServerConfig) -> Result<()> {
    if config.hash.is_empty() {
        return Err(anyhow!("--id/--hash must be non-empty"));
    }

    let repo_root = repo_root();
    let ui_static_root = repo_root.join("video_streamer/ui/static");
    let ui_template_root = repo_root.join("video_streamer/ui/template");
    let ui_template_path = ui_template_root.join("index_mpeg1.html");

    let (tx, _rx) = broadcast::channel::<Bytes>(64);
    let state = AppState {
        hash: config.hash.clone(),
        broadcaster: tx.clone(),
        ui_template_path,
        port: config.port,
    };

    let app = Router::new()
        .route("/ui", get(ui_handler))
        .route(
            "/video_input/",
            post(video_input_handler).put(video_input_handler),
        )
        .route("/ws/:hash", get(ws_handler))
        .nest_service("/static", ServeDir::new(ui_static_root))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .context("Invalid host/port")?;

    if config.debug {
        eprintln!("Binding server at http://{addr}/ui");
        eprintln!("WS endpoint: ws://localhost:{}/ws/{}", config.port, config.hash);
        eprintln!("Input uri: {}", config.uri);
    }

    // Bind first so ffmpeg can connect immediately.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("Failed binding {addr}"))?;

    // Spawn the server.
    let server_task = tokio::spawn(async move {
        axum::serve(listener, app).await
    });

    // Start ffmpeg + camera after the server is bound.
    let mut ffmpeg = start_ffmpeg(&config).context("Failed starting ffmpeg")?;
    let stdin = ffmpeg
        .stdin
        .take()
        .ok_or_else(|| anyhow!("Failed to get ffmpeg stdin"))?;

    let camera_handle = start_camera_writer(&config, stdin).context("Failed starting camera")?;

    // Wait for Ctrl+C.
    tokio::signal::ctrl_c().await.ok();

    // Stop camera writer by killing ffmpeg (writer will see BrokenPipe and exit).
    let _ = ffmpeg.kill();
    let _ = ffmpeg.wait();

    let _ = camera_handle.join();

    // Shut down server task.
    server_task.abort();
    Ok(())
}

async fn ui_handler(State(state): State<AppState>) -> Result<Html<String>, (StatusCode, String)> {
    let source = format!("ws://localhost:{}/ws/{}", state.port, state.hash);

    let template = std::fs::read_to_string(&state.ui_template_path)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let rendered = template.replace("{{ source }}", &source);
    Ok(Html(rendered))
}

async fn ws_handler(
    Path(hash): Path<String>,
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
) -> Response {
    if hash != state.hash {
        return StatusCode::NOT_FOUND.into_response();
    }

    ws.on_upgrade(move |socket| async move {
        let mut rx = state.broadcaster.subscribe();
        let (mut sender, mut receiver) = socket.split();

        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Ok(bytes) => {
                            if sender.send(axum::extract::ws::Message::Binary(bytes.to_vec())).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(_) => break,
                    }
                }
                incoming = receiver.next() => {
                    match incoming {
                        Some(Ok(axum::extract::ws::Message::Close(_))) | None => {
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(_)) => break,
                    }
                }
            }
        }
    })
}

async fn video_input_handler(State(state): State<AppState>, body: Body) -> impl IntoResponse {
    let mut stream = body.into_data_stream();

    while let Some(Ok(chunk)) = stream.next().await {
        // broadcast ignores if no listeners
        let _ = state.broadcaster.send(chunk);
    }

    StatusCode::OK
}

fn start_ffmpeg(config: &ServerConfig) -> Result<Child> {
    // Determine source size (camera size) and output size.
    let source_size = if config.uri == "test" {
        let repo_root = repo_root();
        let img_path = repo_root.join("video_streamer/core/fakeimg.jpg");
        test_camera::probe_size(&img_path)?
    } else if config.uri.starts_with("redis") {
        redis_camera::probe_size(&config.uri, &config.in_redis_channel)?
    } else {
        return Err(anyhow!("Unsupported uri (Rust server supports only test and redis://...): {}", config.uri));
    };

    let out_size = if config.size.0 != 0 { config.size } else { source_size };

    let source_size_str = format!("{}:{}", source_size.0, source_size.1);
    let out_size_str = format!("{}:{}", out_size.0, out_size.1);

    let vf = if config.vflip {
        format!("scale={},vflip", out_size_str)
    } else {
        format!("scale={}", out_size_str)
    };

    let url = format!("http://127.0.0.1:{}/video_input/", config.port);

    let mut cmd = Command::new(&config.ffmpeg);
    cmd.args([
        "-f",
        "rawvideo",
        "-pixel_format",
        "rgb24",
        "-s",
        &source_size_str,
        "-i",
        "-",
        "-f",
        "mpegts",
        "-q:v",
        &config.quality.to_string(),
        "-vf",
        &vf,
        "-vcodec",
        "mpeg1video",
        &url,
    ])
    .stdin(Stdio::piped())
    .stdout(Stdio::null())
    .stderr(if config.debug { Stdio::inherit() } else { Stdio::null() });

    cmd.spawn().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            anyhow!(
                "ffmpeg not found (tried '{}'). Install ffmpeg or pass --ffmpeg /path/to/ffmpeg",
                config.ffmpeg
            )
        } else {
            anyhow!(e).context("Failed spawning ffmpeg")
        }
    })
}

fn start_camera_writer(
    config: &ServerConfig,
    mut ffmpeg_stdin: std::process::ChildStdin,
) -> Result<thread::JoinHandle<()>> {
    let uri = config.uri.clone();
    let channel = config.in_redis_channel.clone();
    let debug = config.debug;

    let handle = thread::spawn(move || {
        if uri == "test" {
            let repo_root = repo_root();
            let img_path = repo_root.join("video_streamer/core/fakeimg.jpg");
            let (w, h) = match test_camera::probe_size(&img_path) {
                Ok(s) => s,
                Err(_) => return,
            };
            let frame = match test_camera::load_rgb_frame(&img_path) {
                Ok(f) => f,
                Err(_) => return,
            };

            let frame_size = (w * h * 3) as usize;
            if frame.len() < frame_size {
                return;
            }

            let sleep = Duration::from_millis(50);
            loop {
                if ffmpeg_stdin.write_all(&frame[..frame_size]).is_err() {
                    break;
                }
                thread::sleep(sleep);
            }
        } else if uri.starts_with("redis") {
            // Reconnect loop; exits on BrokenPipe.
            let mut last_log = Instant::now() - Duration::from_secs(60);
            let mut failures: u64 = 0;
            loop {
                match redis_camera::stream_frames(&uri, &channel, &mut ffmpeg_stdin) {
                    Ok(()) => return,
                    Err(err) => {
                        // BrokenPipe: ffmpeg stopped.
                        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
                            if io_err.kind() == std::io::ErrorKind::BrokenPipe {
                                return;
                            }
                        }

                        failures += 1;

                        if debug {
                            // Many installations have transient TCP resets; treat as expected.
                            // Throttle logging so debug mode remains readable.
                            if last_log.elapsed() > Duration::from_secs(5) {
                                eprintln!(
                                    "Redis stream error (will retry, failures={}): {err:#}",
                                    failures
                                );
                                last_log = Instant::now();
                            }
                        }

                        thread::sleep(Duration::from_millis(200));
                    }
                }
            }
        }
    });

    Ok(handle)
}

fn repo_root() -> PathBuf {
    // rust/ is at repo_root/rust
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .to_path_buf()
}

// No env-based overrides needed.
