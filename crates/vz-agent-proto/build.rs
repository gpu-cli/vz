fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_paths = &["proto/agent.proto", "proto/runtime_v2.proto"];
    let out_dir = "src/generated";

    // Re-run generation if any proto changes.
    for path in proto_paths {
        println!("cargo:rerun-if-changed={path}");
    }

    // Only regenerate when explicitly requested; default builds use
    // checked-in output.
    if std::env::var("GENERATE_PROTOS").is_err() {
        println!("cargo:warning=Skipping proto generation (use GENERATE_PROTOS=1 to regenerate)");
        return Ok(());
    }

    // Ensure vendored protoc is used so local/CI builds match.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    std::fs::create_dir_all(out_dir)?;

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(out_dir)
        .compile_protos(proto_paths, &["proto"])?;

    Ok(())
}
