package jamesl.reactive.msgpack;

import java.math.BigInteger;

/**
 * @param <T>
 * @author jamesl
 * @since 1.0
 */
public interface ElementParser<T> {
    /**
     * Indicates whether {@link Unpacker} should invoke {@link ElementParser#onString(String)} when it
     * receives a {@code null} value. By default {@link ElementParser#onNull()} will be called but some
     * implementations may prefer to override this behaviour.
     *
     * @return
     */
    boolean isRoutingNullsViaOnStringEnabled();

    ElementParser<T> onArrayHeader(int numberOfElements);
    ElementParser<T> onBigInteger(BigInteger value);
    ElementParser<T> onBoolean(boolean value);
    ElementParser<T> onByte(byte value);
    ElementParser<T> onDouble(double value);
    ElementParser<T> onInt(int value);
    ElementParser<T> onLong(long value);
    ElementParser<T> onMapHeader(int numberOfElements);
    ElementParser<T> onNull();
    ElementParser<T> onRaw(byte[] value);
    ElementParser<T> onString(String value);
}
