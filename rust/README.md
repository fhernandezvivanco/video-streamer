# Rust camera producers

This folder contains a small Rust binary used by the Python project for fast frame acquisition.

## Build

From the repo root:

- `cd rust && cargo build --release`

The binary will be at:

- `rust/target/release/video_streamer_camera`

## Run (manual)

- Test image loop:
  - `./rust/target/release/video_streamer_camera test --image-path video_streamer/core/fakeimg.jpg --sleep-ms 50 > /dev/null`

- Redis pubsub:
  - `./rust/target/release/video_streamer_camera redis --uri redis://localhost:6379/ --channel CameraStream > /dev/null`

## Python integration

Python will auto-build the binary on first use (requires `cargo` in `PATH`), or you can point to a prebuilt binary via:

- `VIDEO_STREAMER_RUST_CAMERA_BIN=/path/to/video_streamer_camera`
