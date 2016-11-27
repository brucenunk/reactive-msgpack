package jamesl.reactive.msgpack;

/**
 * @author jamesl
 * @since 1.0
 */
enum WireFormat {
    PositiveFixInt(),
    FixMap(),
    FixArray(),
    FixStr(),
    Nil(),
    NeverUsed(),
    False(),
    True(),
    Bin8(),
    Bin16(),
    Bin32(),
    Float32(),
    Float64(),
    UnsignedInt8(),
    UnsignedInt16(),
    UnsignedInt32(),
    UnsignedInt64(),
    Int8(),
    Int16(),
    Int32(),
    Int64(),
    Str8(),
    Str16(),
    Str32(),
    Array16(),
    Array32(),
    Map16(),
    Map32(),
    NegativeFixInt();

    /**
     * @param b
     * @return
     * @throws UnsupportedWireFormatException
     */
    static WireFormat toWireFormat(byte b) throws UnsupportedWireFormatException {
        if ((b & 0x80) == 0) {
            return WireFormat.PositiveFixInt;
        } else if ((b & 0xf0) == 0x80) {
            return WireFormat.FixMap;
        } else if ((b & 0xf0) == 0x90) {
            return WireFormat.FixArray;
        } else if ((b & 0xe0) == 0xa0) {
            return WireFormat.FixStr;
        } else if ((b & 0xe0) == 0xe0) {
            return WireFormat.NegativeFixInt;
        } else {
            switch (b & 0xff) {
                case 0xc0:
                    return WireFormat.Nil;
                case 0xc1:
                    return WireFormat.NeverUsed;
                case 0xc2:
                    return WireFormat.False;
                case 0xc3:
                    return WireFormat.True;
                case 0xc4:
                    return WireFormat.Bin8;
                case 0xc5:
                    return WireFormat.Bin16;
                case 0xc6:
                    return WireFormat.Bin32;
                case 0xca:
                    return WireFormat.Float32;
                case 0xcb:
                    return WireFormat.Float64;
                case 0xcc:
                    return WireFormat.UnsignedInt8;
                case 0xcd:
                    return WireFormat.UnsignedInt16;
                case 0xce:
                    return WireFormat.UnsignedInt32;
                case 0xcf:
                    return WireFormat.UnsignedInt64;
                case 0xd0:
                    return WireFormat.Int8;
                case 0xd1:
                    return WireFormat.Int16;
                case 0xd2:
                    return WireFormat.Int32;
                case 0xd3:
                    return WireFormat.Int64;
                case 0xd9:
                    return WireFormat.Str8;
                case 0xda:
                    return WireFormat.Str16;
                case 0xdb:
                    return WireFormat.Str32;
                case 0xdc:
                    return WireFormat.Array16;
                case 0xdd:
                    return WireFormat.Array32;
                case 0xde:
                    return WireFormat.Map16;
                case 0xdf:
                    return WireFormat.Map32;
                default:
                    throw new UnsupportedWireFormatException(b);
            }
        }
    }
}
