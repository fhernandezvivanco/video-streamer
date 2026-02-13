use std::{
    io::{self, Write},
    path::PathBuf,
    thread,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

mod redis_camera;
mod test_camera;

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Subscribe to Redis pubsub and output raw rgb24 frames to stdout
    Redis {
        /// Redis URI, e.g. redis://localhost:6379/
        #[arg(long)]
        uri: String,

        /// Pubsub channel name
        #[arg(long)]
        channel: String,

        /// Reconnect delay (ms) after errors
        #[arg(long, default_value_t = 500)]
        reconnect_ms: u64,
    },

    /// Loop an image file as raw rgb24 frames to stdout
    Test {
        /// Path to the test image (e.g. video_streamer/core/fakeimg.jpg)
        #[arg(long)]
        image_path: PathBuf,

        /// Frame interval (ms)
        #[arg(long, default_value_t = 50)]
        sleep_ms: u64,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Redis {
            uri,
            channel,
            reconnect_ms,
        } => redis_loop(&uri, &channel, reconnect_ms),
        Command::Test {
            image_path,
            sleep_ms,
        } => test_loop(image_path, sleep_ms),
    }
}

fn redis_loop(uri: &str, channel: &str, reconnect_ms: u64) -> Result<()> {
    let stdout = io::stdout();
    let mut out = stdout.lock();

    loop {
        match redis_camera::stream_frames(uri, channel, &mut out) {
            Ok(()) => return Ok(()),
            Err(err) => {
                // BrokenPipe means the consumer (ffmpeg) went away.
                if let Some(io_err) = err.downcast_ref::<io::Error>() {
                    if io_err.kind() == io::ErrorKind::BrokenPipe {
                        return Ok(());
                    }
                }
                eprintln!("Redis camera error: {err:#}");
                thread::sleep(Duration::from_millis(reconnect_ms));
            }
        }
    }
}

fn test_loop(image_path: PathBuf, sleep_ms: u64) -> Result<()> {
    let frame = test_camera::load_rgb_frame(&image_path)
        .with_context(|| format!("Failed loading test image: {}", image_path.display()))?;

    let stdout = io::stdout();
    let mut out = stdout.lock();

    let sleep = Duration::from_millis(sleep_ms);
    loop {
        if let Err(err) = out.write_all(&frame) {
            if err.kind() == io::ErrorKind::BrokenPipe {
                return Ok(());
            }
            return Err(err).context("Failed writing frame to stdout");
        }
        // flushing every frame is slower; stdout is typically pipe-buffered.
        thread::sleep(sleep);
    }
}
