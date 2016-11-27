package jamesl.reactive.msgpack;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @param <T>
 * @author jamesl
 * @since 1.0
 */
public class Unpacker<T> {
    private static final int MAX_UNKNOWN_FRAME_LENGTH = 5;
    private static final int UNKNOWN_FRAME_LENGTH = -1;

    private static final Logger logger = LoggerFactory.getLogger(Unpacker.class);
    private ElementParser<T> elementParser;
    private final Queue<T> outputQueue;
    private ByteBuffer splitFrameBuffer;

    public Unpacker(ElementParserFactory<T> elementParserFactory) {
        this.outputQueue = new ArrayDeque<>();
        this.elementParser = elementParserFactory.firstElementParser(output -> outputQueue.offer(output));
    }

    public Publisher<T> unpack(ByteBuffer input) {
        return Flux.create(sink -> {
            logger.debug("received input = {}, splitFrameBuffer = {}", input, splitFrameBuffer);

            if (splitFrameBuffer != null) {
                completeSplitFrame(input);
            }

            while (input.hasRemaining()) {
                int startOfFrame = input.position();
                int numberOfBytesInFrame = determineNumberOfBytesInFrame(input, startOfFrame, input.limit());

                if (numberOfBytesInFrame == UNKNOWN_FRAME_LENGTH || input.remaining() < numberOfBytesInFrame) {
                    splitFrameBuffer = ByteBuffer.allocate(numberOfBytesInFrame == UNKNOWN_FRAME_LENGTH ? MAX_UNKNOWN_FRAME_LENGTH : numberOfBytesInFrame);
                    splitFrameBuffer.put(copyBuffer(input));
                    logger.trace("split frame detected - splitFrameBuffer = {}", splitFrameBuffer);
                    break;
                }

                elementParser = parseElement(input, startOfFrame);
                input.position(startOfFrame + numberOfBytesInFrame);
            }

            logger.debug("consumed input = {}, splitFrameBuffer = {}, outputQueue = {}", input, splitFrameBuffer, outputQueue.size());

            T output;
            while ((output = outputQueue.poll()) != null) {
                sink.next(output);
            }

            sink.complete();
        });
    }

    /**
     * @param sourceBuffer
     * @return
     */
    private ByteBuffer copyBuffer(ByteBuffer sourceBuffer) {
        ByteBuffer slice = sourceBuffer.slice();
        ByteBuffer copyBuffer = ByteBuffer.allocate(slice.limit()).put(slice.asReadOnlyBuffer());

        copyBuffer.flip();
        return copyBuffer;
    }

    /**
     * @param inputBuffer
     */
    private void completeSplitFrame(ByteBuffer inputBuffer) {
        if (attemptToDetermineSplitFrameLength(inputBuffer)) {
            logger.trace("splitFrameBuffer = {}", splitFrameBuffer);

            while (splitFrameBuffer.hasRemaining() && inputBuffer.hasRemaining()) {
                splitFrameBuffer.put(inputBuffer.get());
            }

            if (!splitFrameBuffer.hasRemaining()) {
                logger.trace("split frame complete - splitFrameBuffer = {}", splitFrameBuffer);
                elementParser = parseElement(splitFrameBuffer, 0);
                splitFrameBuffer = null;
            }
        }
    }

    /**
     * Attempts to determine the length of the current split frame.
     *
     * @param inputBuffer
     * @return
     */
    private boolean attemptToDetermineSplitFrameLength(ByteBuffer inputBuffer) {
        int numberOfBytesInFrame = determineNumberOfBytesInFrame(splitFrameBuffer, 0, splitFrameBuffer.position());

        if (numberOfBytesInFrame != UNKNOWN_FRAME_LENGTH) {
            return true;
        }

        while (inputBuffer.hasRemaining()) {
            splitFrameBuffer.put(inputBuffer.get());
            numberOfBytesInFrame = determineNumberOfBytesInFrame(splitFrameBuffer, 0, splitFrameBuffer.position());
            logger.trace("attempting to determine split frame length - numberOfBytesInFrame = {}, splitFrameBuffer = {}", numberOfBytesInFrame, splitFrameBuffer);

            if (numberOfBytesInFrame != UNKNOWN_FRAME_LENGTH) {
                ByteBuffer copy = splitFrameBuffer;
                copy.flip();

                splitFrameBuffer = ByteBuffer.allocate(numberOfBytesInFrame).put(copy);
                logger.trace("expanding split frame inputBuffer - numberOfBytesInFrame = {}, splitFrameBuffer = {}", numberOfBytesInFrame, splitFrameBuffer);
                return true;
            }
        }

        return false;
    }

    /**
     * Attempts to determine the number of bytes in the next frame.
     *
     * @param buffer
     * @param offset
     * @param limit
     * @return
     */
    private int determineNumberOfBytesInFrame(ByteBuffer buffer, int offset, int limit) {
        byte b = buffer.get(offset);
        WireFormat wireFormat = WireFormat.toWireFormat(b);

        int numberOfBytesInFrame;
        switch (wireFormat) {
            case False:
            case FixArray:
            case FixMap:
            case PositiveFixInt:
            case NegativeFixInt:
            case NeverUsed:
            case Nil:
            case True:
                numberOfBytesInFrame = 1;
                break;
            case Int8:
            case UnsignedInt8:
                numberOfBytesInFrame = 2;
                break;
            case Array16:
            case Int16:
            case Map16:
            case UnsignedInt16:
                numberOfBytesInFrame = 3;
                break;
            case Array32:
            case Float32:
            case Int32:
            case Map32:
            case UnsignedInt32:
                numberOfBytesInFrame = 5;
                break;
            case Float64:
            case Int64:
            case UnsignedInt64:
                numberOfBytesInFrame = 9;
                break;
            case Bin8:
            case Str8:
                if (offset + 2 <= limit) {
                    numberOfBytesInFrame = buffer.get(offset + 1) + 2;
                } else {
                    numberOfBytesInFrame = UNKNOWN_FRAME_LENGTH;
                }
                break;
            case Bin16:
            case Str16:
                if (offset + 3 <= limit) {
                    numberOfBytesInFrame = buffer.getShort(offset + 1) + 3;
                } else {
                    numberOfBytesInFrame = UNKNOWN_FRAME_LENGTH;
                }
                break;
            case Bin32:
            case Str32:
                if (offset + 5 <= limit) {
                    numberOfBytesInFrame = buffer.getInt(offset + 1) + 5;
                } else {
                    numberOfBytesInFrame = UNKNOWN_FRAME_LENGTH;
                }
                break;
            case FixStr:
                numberOfBytesInFrame = (buffer.get(offset) & 0x1f) + 1;
                break;
            default:
                throw new RuntimeException("unknown wireformat: " + wireFormat);
        }

        return numberOfBytesInFrame;
    }

    /**
     * Parses the "element" starting at {@code startOffset} in {@code buffer} and returns
     * an {@link ElementParser} for the next element.
     *
     * @param buffer
     * @param startOffset
     * @return
     */
    private ElementParser<T> parseElement(ByteBuffer buffer, int startOffset) {
        WireFormat wireFormat = WireFormat.toWireFormat(buffer.get(startOffset));
        logger.trace("parsing element from {} with wire format {} using {}", startOffset, wireFormat, elementParser);

        switch (wireFormat) {
            case Array16:
                return elementParser.onArrayHeader(buffer.getShort(startOffset + 1));
            case Array32:
                return elementParser.onArrayHeader(buffer.getInt(startOffset + 1));
            case Bin8:
                return elementParser.onRaw(raw(buffer, startOffset + 2, buffer.get(startOffset + 1)));
            case Bin16:
                return elementParser.onRaw(raw(buffer, startOffset + 3, buffer.getShort(startOffset + 1)));
            case Bin32:
                return elementParser.onRaw(raw(buffer, startOffset + 5, buffer.getInt(startOffset + 1)));
            case False:
                return elementParser.onBoolean(false);
            case FixArray:
                return elementParser.onArrayHeader(buffer.get(startOffset) & 0x0f);
            case FixMap:
                return elementParser.onMapHeader(buffer.get(startOffset) & 0x0f);
            case FixStr:
                return elementParser.onString(str(buffer, startOffset + 1, buffer.get(startOffset) & 0x1f));
            case Float32:
                return elementParser.onDouble(buffer.getFloat(startOffset + 1));
            case Float64:
                return elementParser.onDouble(buffer.getDouble(startOffset + 1));
            case Int8:
                return elementParser.onByte(buffer.get(startOffset + 1));
            case Int16:
                return elementParser.onInt(buffer.getShort(startOffset + 1));
            case Int32:
                return elementParser.onInt(buffer.getInt(startOffset + 1));
            case Int64:
                return elementParser.onLong(buffer.getLong(startOffset + 1));
            case Map16:
                return elementParser.onMapHeader(buffer.getShort(startOffset + 1));
            case Map32:
                return elementParser.onMapHeader(buffer.getInt(startOffset + 1));
            case NegativeFixInt:
                return elementParser.onByte(buffer.get(startOffset));
            case Nil:
                if (elementParser.isRoutingNullsViaOnStringEnabled()) {
                    return elementParser.onString(null);
                } else {
                    return elementParser.onNull();
                }
            case PositiveFixInt:
                return elementParser.onByte(buffer.get(startOffset));
            case Str8:
                return elementParser.onString(str(buffer, startOffset + 2, buffer.get(startOffset + 1)));
            case Str16:
                return elementParser.onString(str(buffer, startOffset + 3, buffer.getShort(startOffset + 1)));
            case Str32:
                return elementParser.onString(str(buffer, startOffset + 5, buffer.getInt(startOffset + 1)));
            case True:
                return elementParser.onBoolean(true);
            case UnsignedInt8:
                return elementParser.onInt(buffer.get(startOffset + 1) & 0xff);
            case UnsignedInt16:
                return elementParser.onInt(buffer.getShort(startOffset + 1) & 0xffff);
            case UnsignedInt32:
                int tmp = buffer.getInt(startOffset + 1);
                if (tmp < 0) {
                    return elementParser.onLong((tmp & 0x7fffffff) + 0x80000000L);
                } else {
                    return elementParser.onLong(tmp);
                }
            case UnsignedInt64:
                long tmp2 = buffer.getLong(startOffset + 1);
                if (tmp2 < 0) {
                    return elementParser.onBigInteger(BigInteger.valueOf(tmp2 + Long.MAX_VALUE + 1L).setBit(63));
                } else {
                    return elementParser.onBigInteger(BigInteger.valueOf(tmp2));
                }
            default:
                throw new UnsupportedWireFormatException(wireFormat);
        }
    }

    /**
     * Reads raw bytes from the {@code buffer}.
     *
     * @param buffer the buffer.
     * @param offset the start offset.
     * @param length the number of bytes to read.
     * @return
     */
    private byte[] raw(ByteBuffer buffer, int offset, int length) {
        byte[] rawBytes = new byte[length];
        for (int i = 0; i < length; i++) {
            rawBytes[i] = buffer.get(offset + i);
        }

        return rawBytes;
    }

    /**
     * Reads raw bytes from the {@code buffer} and returns them as a {@link String}.
     *
     * @param buffer the buffer.
     * @param offset the start offset.
     * @param length the number of bytes to read.
     * @return
     */
    private String str(ByteBuffer buffer, int offset, int length) {
        return new String(raw(buffer, offset, length), StandardCharsets.UTF_8);
    }
}
