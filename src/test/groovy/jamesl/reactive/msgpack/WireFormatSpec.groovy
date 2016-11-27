package jamesl.reactive.msgpack

import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author jamesl
 * @since 1.0
 *
 * msgpack spec: https://github.com/msgpack/msgpack/blob/master/spec.md
 */
class WireFormatSpec extends Specification {
    @Unroll
    def "wire format should be positive fixint for #value"(int value) {
        expect:
        WireFormat.toWireFormat(value as byte) == WireFormat.PositiveFixInt

        where:
        value << (0x00..0x7f)
    }

    @Unroll
    def "wire format should be fixmap for #value"(int value) {
        expect:
        WireFormat.toWireFormat(value as byte) == WireFormat.FixMap

        where:
        value << (0x80..0x8f)
    }

    @Unroll
    def "wire format should be fixarray for #value"(int value) {
        expect:
        WireFormat.toWireFormat(value as byte) == WireFormat.FixArray

        where:
        value << (0x90..0x9f)
    }

    @Unroll
    def "wire format should be fixstr for #value"(int value) {
        expect:
        WireFormat.toWireFormat(value as byte) == WireFormat.FixStr

        where:
        value << (0xa0..0xbf)
    }

    @Unroll
    def "wire format should be #wireFormat for #value"(int value, WireFormat wireFormat) {
        expect:
        WireFormat.toWireFormat(value as byte) == wireFormat

        where:
        value | wireFormat
        0xc0  | WireFormat.Nil
        0xc1  | WireFormat.NeverUsed
        0xc2  | WireFormat.False
        0xc3  | WireFormat.True
        0xc4  | WireFormat.Bin8
        0xc5  | WireFormat.Bin16
        0xc6  | WireFormat.Bin32
        0xca  | WireFormat.Float32
        0xcb  | WireFormat.Float64
        0xcc  | WireFormat.UnsignedInt8
        0xcd  | WireFormat.UnsignedInt16
        0xce  | WireFormat.UnsignedInt32
        0xcf  | WireFormat.UnsignedInt64
        0xd0  | WireFormat.Int8
        0xd1  | WireFormat.Int16
        0xd2  | WireFormat.Int32
        0xd3  | WireFormat.Int64
        0xd9  | WireFormat.Str8
        0xda  | WireFormat.Str16
        0xdb  | WireFormat.Str32
        0xdc  | WireFormat.Array16
        0xdd  | WireFormat.Array32
        0xde  | WireFormat.Map16
        0xdf  | WireFormat.Map32
    }

    @Unroll
    def "wire format should be negative fixint for #value"(int value) {
        expect:
        WireFormat.toWireFormat(value as byte) == WireFormat.NegativeFixInt

        where:
        value << (0xe0..0xff)
    }

    @Unroll
    def "wire format #value is an extension and not supported"(int value) {
        when:
        WireFormat.toWireFormat(value as byte);

        then:
        def e = thrown(UnsupportedWireFormatException);
        e.message == "unrecognised wire format - value = ${value as byte}"

        where:
        value | _
        0xc7  | _
        0xc8  | _
        0xc9  | _
        0xd4  | _
        0xd5  | _
        0xd6  | _
        0xd7  | _
        0xd8  | _
    }
}
