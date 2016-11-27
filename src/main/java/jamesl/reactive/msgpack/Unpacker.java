package jamesl.reactive.msgpack;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @param <T>
 * @author jamesl
 * @since 1.0
 */
public class Unpacker<T> {
    private static final Logger logger = LoggerFactory.getLogger(Unpacker.class);
    private static WireFormat[] wireFormats = new WireFormat[256];

    static {
        // JL cache a WireFormat instance for each byte value.
        WireFormatFactory wireFormatFactory = new WireFormatFactory();
        for (int i = 0; i < wireFormats.length; i++) {
            wireFormats[i] = wireFormatFactory.newWireFormat((byte) i);
        }
    }

    private static final int MAX_UNKNOWN_FRAME_LENGTH = 5;
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

                if (numberOfBytesInFrame == WireFormat.UNKNOWN_FRAME_LENGTH || input.remaining() < numberOfBytesInFrame) {
                    splitFrameBuffer = ByteBuffer.allocate(numberOfBytesInFrame == WireFormat.UNKNOWN_FRAME_LENGTH ? MAX_UNKNOWN_FRAME_LENGTH : numberOfBytesInFrame);
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

        if (numberOfBytesInFrame != WireFormat.UNKNOWN_FRAME_LENGTH) {
            return true;
        }

        while (inputBuffer.hasRemaining()) {
            splitFrameBuffer.put(inputBuffer.get());
            numberOfBytesInFrame = determineNumberOfBytesInFrame(splitFrameBuffer, 0, splitFrameBuffer.position());
            logger.trace("attempting to determine split frame length - numberOfBytesInFrame = {}, splitFrameBuffer = {}", numberOfBytesInFrame, splitFrameBuffer);

            if (numberOfBytesInFrame != WireFormat.UNKNOWN_FRAME_LENGTH) {
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
     * @param startOffset
     * @param limit
     * @return
     */
    private int determineNumberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit) {
        return wireFormat(buffer.get(startOffset)).numberOfBytesInFrame(buffer, startOffset, limit);
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
        return wireFormat(buffer.get(startOffset)).parseElement(buffer, startOffset, elementParser);
    }

    /**
     * Returns a {@link WireFormat} for the specified {@code head}.
     *
     * @param head
     * @return
     */
    private WireFormat wireFormat(byte head) {
        return wireFormats[head & 0xff];
    }
}
