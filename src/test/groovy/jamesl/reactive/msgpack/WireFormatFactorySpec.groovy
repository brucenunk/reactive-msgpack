package jamesl.reactive.msgpack

import spock.lang.Specification
import spock.lang.Unroll

/**
 * @author jamesl
 * @since 1.0
 *
 * msgpack spec: https://github.com/msgpack/msgpack/blob/master/spec.md
 */
class WireFormatFactorySpec extends Specification {
    WireFormatFactory wireFormatFactory

    def setup() {
        wireFormatFactory = new WireFormatFactory()
    }

    @Unroll
    def "wire format should be positive fixint for #value"(int value) {
        when:
        def wireFormat = wireFormatFactory.newWireFormat(value as byte)

        then:
        wireFormat instanceof WireFormatFactory.PositiveFixInt

        where:
        value << (0x00..0x7f)
    }

    @Unroll
    def "wire format should be fixmap for #value"(int value) {
        when:
        def wireFormat = wireFormatFactory.newWireFormat(value as byte)

        then:
        wireFormat instanceof WireFormatFactory.FixMap

        where:
        value << (0x80..0x8f)
    }

    @Unroll
    def "wire format should be fixarray for #value"(int value) {
        when:
        def wireFormat = wireFormatFactory.newWireFormat(value as byte)

        then:
        wireFormat instanceof WireFormatFactory.FixArray

        where:
        value << (0x90..0x9f)
    }

    @Unroll
    def "wire format should be fixstr for #value"(int value) {
        when:
        def wireFormat = wireFormatFactory.newWireFormat(value as byte)

        then:
        wireFormat instanceof WireFormatFactory.FixStr

        where:
        value << (0xa0..0xbf)
    }

    @Unroll
    def "wire format should be #expected.simpleName for #value"(int value, Class<? extends WireFormat> expected) {
        when:
        def wireFormat = wireFormatFactory.newWireFormat(value as byte)

        then:
        wireFormat.class == expected

        where:
        value | expected
        0xc0  | WireFormatFactory.Nil
        0xc1  | WireFormatFactory.NeverUsed
        0xc2  | WireFormatFactory.False
        0xc3  | WireFormatFactory.True
        0xc4  | WireFormatFactory.Bin8
        0xc5  | WireFormatFactory.Bin16
        0xc6  | WireFormatFactory.Bin32
        0xca  | WireFormatFactory.Float32
        0xcb  | WireFormatFactory.Float64
        0xcc  | WireFormatFactory.UnsignedInt8
        0xcd  | WireFormatFactory.UnsignedInt16
        0xce  | WireFormatFactory.UnsignedInt32
        0xcf  | WireFormatFactory.UnsignedInt64
        0xd0  | WireFormatFactory.Int8
        0xd1  | WireFormatFactory.Int16
        0xd2  | WireFormatFactory.Int32
        0xd3  | WireFormatFactory.Int64
        0xd9  | WireFormatFactory.Str8
        0xda  | WireFormatFactory.Str16
        0xdb  | WireFormatFactory.Str32
        0xdc  | WireFormatFactory.Array16
        0xdd  | WireFormatFactory.Array32
        0xde  | WireFormatFactory.Map16
        0xdf  | WireFormatFactory.Map32
    }

    @Unroll
    def "wire format should be negative fixint for #value"(int value) {
        when:
        def wireFormat = wireFormatFactory.newWireFormat(value as byte)

        then:
        wireFormat instanceof WireFormatFactory.NegativeFixInt

        where:
        value << (0xe0..0xff)
    }

    @Unroll
    def "wire format #value is an extension and not supported"(int value) {
        when:
        def wireFormat = wireFormatFactory.newWireFormat(value as byte);

        then:
        wireFormat instanceof WireFormatFactory.NeverUsed

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
