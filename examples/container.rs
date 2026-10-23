//! Inside a Surfra job or app: no credential or URL to set. Writes `rust.txt` and reads it back.
//!
//!     cargo run --example container --features blocking

fn main() -> surfra::Result<()> {
    let weights = surfra::blocking::Volume::new("weights");
    weights.write("from/rust.txt", "rust")?;
    let back = weights.read("from/rust.txt")?;
    println!("rust wrote and read {} bytes", back.len());
    Ok(())
}
