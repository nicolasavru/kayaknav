//! Download NOAA harmonic constants for every station, or convert between
//! plaintext (JSON) and binary (Postcard) on-disk forms without hitting
//! the network.
//!
//! Usage:
//!   harcon_store download --output <path>      [--concurrency N] [--delay-ms M]
//!   harcon_store convert  --input <path>       --output <path>
//!
//! Format is auto-detected by file extension: `.json` is plaintext,
//! `.zst` is zstd-compressed bitcode binary (written at level 22),
//! anything else (`.bin`, …) is raw bitcode binary.

use std::path::Path;

use noaa_tides::Client;
use noaa_tides::HarconStore;
use noaa_tides::prelude::*;

fn print_usage() {
    eprintln!(
        "usage:\n  \
         harcon_store download --output <path> [--concurrency N] [--delay-ms M]\n  \
         harcon_store convert  --input <path> --output <path>"
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Format {
    Plaintext,
    Binary,
    BinaryZstd,
}

impl Format {
    fn from_path(path: &Path) -> Self {
        match path.extension().and_then(|e| e.to_str()) {
            Some("json") => Format::Plaintext,
            Some("zst") => Format::BinaryZstd,
            _ => Format::Binary,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Format::Plaintext => "plaintext JSON",
            Format::Binary => "binary bitcode",
            Format::BinaryZstd => "zstd-compressed bitcode (level 22)",
        }
    }
}

fn write_store(path: &Path, store: &HarconStore) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create parent for {}", path.display()))?;
        }
    }
    let fmt = Format::from_path(path);
    let bytes = match fmt {
        Format::Plaintext => store.to_plaintext()?.into_bytes(),
        Format::Binary => store.to_binary()?,
        Format::BinaryZstd => store.to_binary_zstd(22)?,
    };
    std::fs::write(path, &bytes).with_context(|| format!("write {}", path.display()))?;
    eprintln!(
        "wrote {} ({} bytes, {})",
        path.display(),
        bytes.len(),
        fmt.label()
    );
    Ok(())
}

fn read_store(path: &Path) -> Result<HarconStore> {
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let fmt = Format::from_path(path);
    let store = match fmt {
        Format::Plaintext => {
            let s = std::str::from_utf8(&bytes).context("input is not UTF-8 plaintext")?;
            HarconStore::from_plaintext(s)?
        },
        Format::Binary => HarconStore::from_binary(&bytes)?,
        Format::BinaryZstd => HarconStore::from_binary_zstd(&bytes)?,
    };
    eprintln!(
        "read {}: {} ({} bytes, {})",
        path.display(),
        store.summary(),
        bytes.len(),
        fmt.label()
    );
    Ok(store)
}

fn get_flag<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
}

async fn cmd_download(args: &[String]) -> Result<()> {
    let output =
        get_flag(args, "--output").ok_or_else(|| anyhow!("--output <path> is required"))?;
    let concurrency: usize = get_flag(args, "--concurrency")
        .map(|s| s.parse())
        .transpose()
        .context("--concurrency")?
        .unwrap_or(6);
    let delay_ms: u64 = get_flag(args, "--delay-ms")
        .map(|s| s.parse())
        .transpose()
        .context("--delay-ms")?
        .unwrap_or(0);

    eprintln!(
        "downloading with concurrency={concurrency} delay_ms={delay_ms} \
         (cache dir: {})",
        Client::new()
            .cache_dir()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<none>".into())
    );
    let client = Client::new();
    let store = HarconStore::download(&client, concurrency, delay_ms).await?;
    eprintln!("assembled store: {}", store.summary());
    write_store(Path::new(output), &store)
}

fn cmd_convert(args: &[String]) -> Result<()> {
    let input = get_flag(args, "--input").ok_or_else(|| anyhow!("--input <path> is required"))?;
    let output =
        get_flag(args, "--output").ok_or_else(|| anyhow!("--output <path> is required"))?;
    let store = read_store(Path::new(input))?;
    write_store(Path::new(output), &store)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args: Vec<String> = std::env::args().collect();
    let cmd = match args.get(1) {
        Some(c) => c.as_str(),
        None => {
            print_usage();
            return Err(anyhow!("missing subcommand"));
        },
    };
    let rest = &args[2..];
    match cmd {
        "download" => cmd_download(rest).await,
        "convert" => cmd_convert(rest),
        "-h" | "--help" => {
            print_usage();
            Ok(())
        },
        other => {
            print_usage();
            Err(anyhow!("unknown subcommand: {other}"))
        },
    }
}
