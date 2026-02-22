fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_path = "proto/agent.proto";
    let out_dir = "src/generated";

    // Re-run generation if the proto changes.
    println!("cargo:rerun-if-changed={proto_path}");

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
        .compile_protos(&[proto_path], &["proto"])?;

    Ok(())
}
