fn main() {
    // Link Virtualization.framework
    #[cfg(target_os = "macos")]
    println!("cargo:rustc-link-lib=framework=Virtualization");
}
