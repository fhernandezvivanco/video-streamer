use std::io::Write;

use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt};

const MD3_HEADER_SIZE: usize = 24;

// Python struct: "<HiiHHQH" => 2 + 4 + 4 + 2 + 2 + 8 + 2 = 24 bytes
#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
struct Md3Header {
    _magic: u16,
    width: i32,
    height: i32,
    _s1: u16,
    _s2: u16,
    _ts: u64,
    _seq: u16,
}

fn parse_header(buf: &[u8]) -> Result<Md3Header> {
    if buf.len() < MD3_HEADER_SIZE {
        return Err(anyhow!("Redis message too short for MD3 header"));
    }

    let mut cursor = std::io::Cursor::new(&buf[..MD3_HEADER_SIZE]);
    let header = Md3Header {
        _magic: cursor.read_u16::<LittleEndian>()?,
        width: cursor.read_i32::<LittleEndian>()?,
        height: cursor.read_i32::<LittleEndian>()?,
        _s1: cursor.read_u16::<LittleEndian>()?,
        _s2: cursor.read_u16::<LittleEndian>()?,
        _ts: cursor.read_u64::<LittleEndian>()?,
        _seq: cursor.read_u16::<LittleEndian>()?,
    };

    if header.width <= 0 || header.height <= 0 {
        return Err(anyhow!("Invalid frame size {}x{}", header.width, header.height));
    }

    Ok(header)
}

pub fn stream_frames(uri: &str, channel: &str, out: &mut impl Write) -> Result<()> {
    let client = redis::Client::open(uri).with_context(|| format!("Invalid redis uri: {uri}"))?;
    let mut conn = client
        .get_connection()
        .with_context(|| format!("Failed connecting to redis at {uri}"))?;

    let mut pubsub = conn.as_pubsub();
    pubsub
        .subscribe(channel)
        .with_context(|| format!("Failed subscribing to channel {channel}"))?;

    loop {
        let msg = pubsub.get_message().context("Failed receiving redis pubsub message")?;
        let data = msg.get_payload_bytes();
        if data.len() < MD3_HEADER_SIZE {
            continue;
        }

        let header = match parse_header(data) {
            Ok(h) => h,
            Err(_) => continue,
        };

        let width = header.width as usize;
        let height = header.height as usize;
        let expected_rgb = width * height * 3;
        let expected_gray = width * height;

        let payload = &data[MD3_HEADER_SIZE..];

        if payload.len() >= expected_rgb {
            out.write_all(&payload[..expected_rgb])
                .context("Failed writing rgb24 frame")?;
        } else if payload.len() >= expected_gray {
            let gray = &payload[..expected_gray];
            let mut rgb = Vec::with_capacity(expected_rgb);
            for &px in gray {
                rgb.push(px);
                rgb.push(px);
                rgb.push(px);
            }
            out.write_all(&rgb).context("Failed writing gray->rgb24 frame")?;
        } else {
            continue;
        }
    }
}

pub fn probe_size(uri: &str, channel: &str) -> Result<(u32, u32)> {
    let client = redis::Client::open(uri).with_context(|| format!("Invalid redis uri: {uri}"))?;
    let mut conn = client
        .get_connection()
        .with_context(|| format!("Failed connecting to redis at {uri}"))?;

    let mut pubsub = conn.as_pubsub();
    pubsub
        .subscribe(channel)
        .with_context(|| format!("Failed subscribing to channel {channel}"))?;

    loop {
        let msg = pubsub.get_message().context("Failed receiving redis pubsub message")?;
        let data = msg.get_payload_bytes();
        if data.len() < MD3_HEADER_SIZE {
            continue;
        }
        if let Ok(header) = parse_header(data) {
            return Ok((header.width as u32, header.height as u32));
        }
    }
}
