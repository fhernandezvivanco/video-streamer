use std::path::Path;

use anyhow::{Context, Result};

pub fn probe_size(image_path: &Path) -> Result<(u32, u32)> {
    let img = image::open(image_path)
        .with_context(|| format!("Unable to open image {}", image_path.display()))?;
    Ok((img.width(), img.height()))
}

pub fn load_rgb_frame(image_path: &Path) -> Result<Vec<u8>> {
    let img = image::open(image_path)
        .with_context(|| format!("Unable to open image {}", image_path.display()))?;

    let rgb = img.to_rgb8();
    Ok(rgb.into_raw())
}
