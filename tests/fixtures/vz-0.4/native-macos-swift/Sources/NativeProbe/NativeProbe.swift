import Foundation

struct ProbeError: Error, CustomStringConvertible {
    let description: String
}

func readCommand(_ path: String, _ arguments: [String]) throws -> String {
    let process = Process()
    process.executableURL = URL(fileURLWithPath: path)
    process.arguments = arguments
    let output = Pipe()
    process.standardOutput = output
    try process.run()
    let bytes = output.fileHandleForReading.readDataToEndOfFile()
    process.waitUntilExit()
    guard process.terminationStatus == 0, let text = String(data: bytes, encoding: .utf8) else {
        throw ProbeError(description: "probe command failed: \(path)")
    }
    return text.trimmingCharacters(in: .whitespacesAndNewlines)
}

func requireVirtualMac(_ model: String) throws {
    guard model.hasPrefix("VirtualMac") else {
        throw ProbeError(description: "expected a native macOS VM; observed hardware model \(model)")
    }
}

@main
struct NativeProbe {
    static func main() {
        do {
            try run()
        } catch {
            FileHandle.standardError.write(Data("native-probe: \(error)\n".utf8))
            exit(1)
        }
    }

    static func run() throws {
        let model = try readCommand("/usr/sbin/sysctl", ["-n", "hw.model"])
        try requireVirtualMac(model)
        let record: [String: Any] = [
            "protocol": "vz-native-macos-swift",
            "protocol_version": 1,
            "os_version": try readCommand("/usr/bin/sw_vers", ["-productVersion"]),
            "os_build": try readCommand("/usr/bin/sw_vers", ["-buildVersion"]),
            "hardware_model": model,
            "pid": ProcessInfo.processInfo.processIdentifier,
        ]
        let bytes = try JSONSerialization.data(withJSONObject: record, options: [.sortedKeys])
        FileHandle.standardOutput.write(bytes)
        FileHandle.standardOutput.write(Data([10]))
    }
}
