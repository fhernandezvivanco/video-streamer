# Rust camera producers

This folder contains a Rust binary that can either:

- run the full MPEG1 HTTP+WebSocket server (no Python), or
- act as a frame producer (used previously by the Python project).

## Build

From the repo root:

- `cd rust && cargo build --release`

The binary will be at:

- `rust/target/release/video_streamer_camera`

## Run (Rust-only server, no Python)

Prerequisite: `ffmpeg` must be installed and available in `PATH`.
If it isn’t, pass an explicit path via `--ffmpeg /path/to/ffmpeg`.

MPEG1 only (equivalent endpoints to the FastAPI server):

- `GET /ui` (HTML player)
- `GET /static/jsmpeg.min.js`
- `POST /video_input/` (ffmpeg pushes MPEG-TS here)
- `WS /ws/{hash}` (browser connects here)

Example (test image):

- `./rust/target/release/video_streamer_camera server --host 0.0.0.0 --port 8000 --uri test --hash stream`

Example (Redis pubsub):

- `./rust/target/release/video_streamer_camera server --port 8000 --uri redis://localhost:6379/ --in-redis-channel CameraStream --hash stream`

## Run (manual)

- Test image loop:
  - `./rust/target/release/video_streamer_camera test --image-path video_streamer/core/fakeimg.jpg --sleep-ms 50 > /dev/null`

- Redis pubsub:
  - `./rust/target/release/video_streamer_camera redis --uri redis://localhost:6379/ --channel CameraStream > /dev/null`

## Python integration

Python will auto-build the binary on first use (requires `cargo` in `PATH`), or you can point to a prebuilt binary via:

- `VIDEO_STREAMER_RUST_CAMERA_BIN=/path/to/video_streamer_camera`
