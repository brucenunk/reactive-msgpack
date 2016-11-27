package jamesl.reactive.msgpack;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author jamesl
 * @since 1.0
 */
public class WireFormatFactory {
    /**
     * Returns a {@link WireFormat} for the specified {@code head}.
     *
     * @param head
     * @return
     */
    WireFormat newWireFormat(byte head) {
        if ((head & 0x80) == 0) {
            return new PositiveFixInt();
        } else if ((head & 0xf0) == 0x80) {
            return new FixMap();
        } else if ((head & 0xf0) == 0x90) {
            return new FixArray();
        } else if ((head & 0xe0) == 0xa0) {
            return new FixStr();
        } else if ((head & 0xe0) == 0xe0) {
            return new NegativeFixInt();
        } else {
            switch (head & 0xff) {
                case 0xc0:
                    return new Nil();
                case 0xc1:
                    return new NeverUsed();
                case 0xc2:
                    return new False();
                case 0xc3:
                    return new True();
                case 0xc4:
                    return new Bin8();
                case 0xc5:
                    return new Bin16();
                case 0xc6:
                    return new Bin32();
                case 0xca:
                    return new Float32();
                case 0xcb:
                    return new Float64();
                case 0xcc:
                    return new UnsignedInt8();
                case 0xcd:
                    return new UnsignedInt16();
                case 0xce:
                    return new UnsignedInt32();
                case 0xcf:
                    return new UnsignedInt64();
                case 0xd0:
                    return new Int8();
                case 0xd1:
                    return new Int16();
                case 0xd2:
                    return new Int32();
                case 0xd3:
                    return new Int64();
                case 0xd9:
                    return new Str8();
                case 0xda:
                    return new Str16();
                case 0xdb:
                    return new Str32();
                case 0xdc:
                    return new Array16();
                case 0xdd:
                    return new Array32();
                case 0xde:
                    return new Map16();
                case 0xdf:
                    return new Map32();
                default:
                    return new NeverUsed();
            }
        }
    }


    /**
     * "simple" implementation with fixed message size.
     */
    static abstract class SimpleWireFormat implements WireFormat {
        private final int numberOfBytesInFrame;

        SimpleWireFormat() {
            this(1);
        }

        SimpleWireFormat(int numberOfBytesInFrame) {
            this.numberOfBytesInFrame = numberOfBytesInFrame;
        }

        @Override
        public int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
            return numberOfBytesInFrame;
        }
    }

    /**
     * "raw" implementation with variable message size.
     */
    static abstract class RawWireFormat implements WireFormat {
        /**
         * Reads raw bytes from the {@code buffer}.
         *
         * @param buffer
         * @param startOffset
         * @param length
         * @return
         */
        byte[] raw(ByteBuffer buffer, int startOffset, int length) {
            byte[] raw = new byte[length];
            for (int i = 0; i < length; i++) {
                raw[i] = buffer.get(startOffset + i);
            }

            return raw;
        }

        /**
         * Reads {@code length} bytes from the {@code buffer} and converts them into a UTF-8 {@link String}.
         *
         * @param buffer
         * @param startOffset
         * @param length
         * @return
         */
        String str(ByteBuffer buffer, int startOffset, int length) {
            return new String(raw(buffer, startOffset, length), StandardCharsets.UTF_8);
        }
    }

    static class Array16 extends SimpleWireFormat {
        Array16() {
            super(3);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onArrayHeader(buffer.getShort(startOffset + 1));
        }
    }

    static class Array32 extends SimpleWireFormat {
        Array32() {
            super(5);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onArrayHeader(buffer.getInt(startOffset + 1));
        }
    }

    static class Bin8 extends RawWireFormat {
        @Override
        public int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
            if (startOffset + 2 <= limit) {
                return buffer.get(startOffset + 1) + 2;
            } else {
                return UNKNOWN_FRAME_LENGTH;
            }
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onRaw(raw(buffer, startOffset + 2, buffer.get(startOffset + 1)));
        }
    }

    static class Bin16 extends RawWireFormat {
        @Override
        public int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
            if (startOffset + 3 <= limit) {
                return buffer.getShort(startOffset + 1) + 3;
            } else {
                return UNKNOWN_FRAME_LENGTH;
            }
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onRaw(raw(buffer, startOffset + 3, buffer.getShort(startOffset + 1)));
        }
    }

    static class Bin32 extends RawWireFormat {
        @Override
        public int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
            if (startOffset + 5 <= limit) {
                return buffer.getInt(startOffset + 1) + 5;
            } else {
                return UNKNOWN_FRAME_LENGTH;
            }
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onRaw(raw(buffer, startOffset + 5, buffer.getInt(startOffset + 1)));
        }
    }

    static class False extends SimpleWireFormat {
        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onBoolean(false);
        }
    }

    static class FixArray extends SimpleWireFormat {
        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onArrayHeader(buffer.get(startOffset) & 0x0f);
        }
    }

    static class FixMap extends SimpleWireFormat {
        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onMapHeader(buffer.get(startOffset) & 0x0f);
        }
    }

    static class FixStr extends RawWireFormat {
        @Override
        public int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
            return (buffer.get(startOffset) & 0x1f) + 1;
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onString(str(buffer, startOffset + 1, buffer.get(startOffset) & 0x1f));
        }
    }

    static class Float32 extends SimpleWireFormat {
        Float32() {
            super(5);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onDouble(buffer.getFloat(startOffset + 1));
        }
    }

    static class Float64 extends SimpleWireFormat {
        Float64() {
            super(9);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onDouble(buffer.getDouble(startOffset + 1));
        }
    }

    static class Int8 extends SimpleWireFormat {
        Int8() {
            super(2);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onByte(buffer.get(startOffset + 1));
        }
    }

    static class Int16 extends SimpleWireFormat {
        Int16() {
            super(3);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onInt(buffer.getShort(startOffset + 1));
        }
    }

    static class Int32 extends SimpleWireFormat {
        Int32() {
            super(5);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onInt(buffer.getInt(startOffset + 1));
        }
    }

    static class Int64 extends SimpleWireFormat {
        Int64() {
            super(9);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onLong(buffer.getLong(startOffset + 1));
        }
    }

    static class Map16 extends SimpleWireFormat {
        Map16() {
            super(3);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onMapHeader(buffer.getShort(startOffset + 1));
        }
    }

    static class Map32 extends SimpleWireFormat {
        Map32() {
            super(5);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onMapHeader(buffer.getInt(startOffset + 1));
        }
    }

    static class NegativeFixInt extends SimpleWireFormat {
        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onByte(buffer.get(startOffset));
        }
    }

    static class NeverUsed extends SimpleWireFormat {
        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser;
        }
    }

    static class Nil extends SimpleWireFormat {
        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            if (elementParser.isRoutingNullsViaOnStringEnabled()) {
                return elementParser.onString(null);
            } else {
                return elementParser.onNull();
            }
        }
    }

    static class PositiveFixInt extends SimpleWireFormat {
        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onByte(buffer.get(startOffset));
        }
    }

    static class Str8 extends RawWireFormat {
        @Override
        public int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
            if (startOffset + 2 <= limit) {
                return buffer.get(startOffset + 1) + 2;
            } else {
                return UNKNOWN_FRAME_LENGTH;
            }
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onString(str(buffer, startOffset + 2, buffer.get(startOffset + 1)));
        }
    }

    static class Str16 extends RawWireFormat {
        @Override
        public int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
            if (startOffset + 3 <= limit) {
                return buffer.getShort(startOffset + 1) + 3;
            } else {
                return UNKNOWN_FRAME_LENGTH;
            }
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onString(str(buffer, startOffset + 3, buffer.getShort(startOffset + 1)));
        }
    }

    static class Str32 extends RawWireFormat {
        @Override
        public int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
            if (startOffset + 5 <= limit) {
                return buffer.getInt(startOffset + 1) + 5;
            } else {
                return UNKNOWN_FRAME_LENGTH;
            }
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onString(str(buffer, startOffset + 5, buffer.getInt(startOffset + 1)));
        }
    }

    static class True extends SimpleWireFormat {
        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onBoolean(true);
        }
    }

    static class UnsignedInt8 extends SimpleWireFormat {
        UnsignedInt8() {
            super(2);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onInt(buffer.get(startOffset + 1) & 0xff);
        }
    }

    static class UnsignedInt16 extends SimpleWireFormat {
        UnsignedInt16() {
            super(3);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            return elementParser.onInt(buffer.getShort(startOffset + 1) & 0xffff);
        }
    }

    static class UnsignedInt32 extends SimpleWireFormat {
        UnsignedInt32() {
            super(5);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            int tmp = buffer.getInt(startOffset + 1);
            if (tmp < 0) {
                return elementParser.onLong((tmp & 0x7fffffff) + 0x80000000L);
            } else {
                return elementParser.onLong(tmp);
            }
        }
    }

    static class UnsignedInt64 extends SimpleWireFormat {
        UnsignedInt64() {
            super(9);
        }

        @Override
        public <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser) {
            long tmp2 = buffer.getLong(startOffset + 1);
            if (tmp2 < 0) {
                return elementParser.onBigInteger(BigInteger.valueOf(tmp2 + Long.MAX_VALUE + 1L).setBit(63));
            } else {
                return elementParser.onBigInteger(BigInteger.valueOf(tmp2));
            }
        }
    }
}