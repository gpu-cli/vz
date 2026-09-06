//! Maintainer tooling: consumes quiescent images prepared separately. This is
//! not a public vz command and does not provision, boot, sign or publish images.
use anyhow::{Context, Result, bail};
use std::path::Path;
use vz_macos_provision::image_delta;

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 4 {
        bail!("usage: image_delta <create|apply> <base.img> <prepared.img|patch.vzdelta> <output>");
    }
    let callback = |progress| -> Result<()> {
        use std::io::Write;
        // Serialize first: writing JSON token by token to unbuffered stderr
        // adds many syscalls to every large-image verification checkpoint.
        let mut line = serde_json::to_vec(&progress)?;
        line.push(b'\n');
        std::io::stderr().lock().write_all(&line)?;
        Ok(())
    };
    let base = Path::new(&args[1]);
    let input = Path::new(&args[2]);
    let output = Path::new(&args[3]);
    let result = match args[0].to_str().context("operation must be UTF-8")? {
        "create" => image_delta::create(base, input, output, 4 * 1024 * 1024, callback)?,
        "apply" => image_delta::apply(base, input, output, callback)?,
        _ => bail!("operation must be create or apply"),
    };
    serde_json::to_writer_pretty(std::io::stdout(), &result)?;
    Ok(())
}
