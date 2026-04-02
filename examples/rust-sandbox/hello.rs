fn main() {
    println!("Hello from a Linux VM on macOS!");
    println!("arch: {}", std::env::consts::ARCH);
    println!("os: {}", std::env::consts::OS);
}
