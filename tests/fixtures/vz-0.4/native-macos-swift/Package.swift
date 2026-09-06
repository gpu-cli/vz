// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "NativeProbe",
    platforms: [.macOS(.v13)],
    products: [.executable(name: "native-probe", targets: ["NativeProbe"])],
    targets: [
        .executableTarget(name: "NativeProbe"),
        .testTarget(name: "NativeProbeTests", dependencies: ["NativeProbe"]),
    ]
)
