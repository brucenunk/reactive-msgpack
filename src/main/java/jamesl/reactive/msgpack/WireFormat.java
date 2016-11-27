package jamesl.reactive.msgpack;

import java.nio.ByteBuffer;

/**
 * @author jamesl
 * @since 1.0
 */
interface WireFormat {
    int UNKNOWN_FRAME_LENGTH = -1;

    /**
     * Returns the number of bytes in the current message frame.
     *
     * @param buffer
     * @param startOffset
     * @param limit
     * @return
     */
    int numberOfBytesInFrame(ByteBuffer buffer, int startOffset, int limit);

    /**
     * Parses the current message frame using the provided {@code elementParser}.
     *
     * @param buffer
     * @param startOffset
     * @param elementParser
     * @param <T>
     * @return the parser to use for the next element.
     */
    <T> ElementParser<T> parseElement(ByteBuffer buffer, int startOffset, ElementParser<T> elementParser);
}
