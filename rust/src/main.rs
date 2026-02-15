use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

mod server;
mod redis_camera;
mod test_camera;

#[derive(Parser, Debug)]
#[command(author, version, about = "MPEG1 HTTP+WebSocket server")]
struct Cli {
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    #[arg(long, default_value_t = 8000)]
    port: u16,

    /// Input source: "test" or "redis://host:port/"
    #[arg(long, default_value = "test")]
    uri: String,

    /// ffmpeg binary (defaults to `ffmpeg` in PATH)
    #[arg(long, default_value = "ffmpeg")]
    ffmpeg: String,

    #[arg(long, default_value_t = 4)]
    quality: u8,

    /// Output size (w,h). Use 0,0 for source size.
    #[arg(long, default_value = "0,0")]
    size: String,

    #[arg(long, default_value_t = false)]
    vflip: bool,

    /// Stream id/hash used in the websocket route: /ws/{hash}
    #[arg(long, default_value = "stream")]
    hash: String,

    /// Channel for RedisCamera to listen to (used only for --uri redis://...)
    #[arg(long, default_value = "CameraStream")]
    in_redis_channel: String,

    /// Enable debug output (prints ffmpeg stderr and server startup info)
    #[arg(long, default_value_t = false)]
    debug: bool,

    /// Path to the test image (used only for --uri test)
    #[arg(long)]
    image_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let (w, h) = parse_size(&cli.size)?;
    let cfg = server::ServerConfig {
        host: cli.host,
        port: cli.port,
        uri: cli.uri,
        ffmpeg: cli.ffmpeg,
        quality: cli.quality,
        size: (w, h),
        vflip: cli.vflip,
        hash: cli.hash,
        debug: cli.debug,
        image_path: cli.image_path,
        in_redis_channel: cli.in_redis_channel,
    };
    server::run(cfg).await
}

fn parse_size(size: &str) -> Result<(u32, u32)> {
    let parts: Vec<&str> = size.split(',').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid --size format, expected w,h"));
    }

    let w: u32 = parts[0].trim().parse().context("Invalid width")?;
    let h: u32 = parts[1].trim().parse().context("Invalid height")?;
    Ok((w, h))
}




