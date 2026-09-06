import Testing
@testable import NativeProbe

@Test func physicalMacCannotSatisfyGuestProbe() throws {
    #expect(throws: ProbeError.self) { try requireVirtualMac("Mac14,12") }
    #expect(throws: ProbeError.self) { try requireVirtualMac("") }
    try requireVirtualMac("VirtualMac2,1")
}
