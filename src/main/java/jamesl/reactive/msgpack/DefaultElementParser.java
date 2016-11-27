package jamesl.reactive.msgpack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * @param <T>
 * @author jamesl
 * @since 1.0
 *
 * Implementation of {@link ElementParser} that just logs methods at TRACE level. It
 * implements {@link ElementParser#isRoutingNullsViaOnStringEnabled()} as {@code false}
 * so will pass {@code null} values to {@link ElementParser#onNull()}.
 */
public abstract class DefaultElementParser<T> implements ElementParser<T> {
    private static final Logger logger = LoggerFactory.getLogger(DefaultElementParser.class);

    @Override
    public boolean isRoutingNullsViaOnStringEnabled() {
        return false;
    }

    @Override
    public ElementParser<T> onArrayHeader(int numberOfElements) {
        logger.trace("onArrayHeader(numberOfElements={})", numberOfElements);
        return this;
    }

    @Override
    public ElementParser<T> onBigInteger(BigInteger value) {
        logger.trace("onBigInteger({})", value);
        return this;
    }

    @Override
    public ElementParser<T> onBoolean(boolean value) {
        logger.trace("onBoolean({})", value);
        return this;
    }

    @Override
    public ElementParser<T> onByte(byte value) {
        logger.trace("onByte({})", value);
        return this;
    }

    @Override
    public ElementParser<T> onDouble(double value) {
        logger.trace("onDouble({})", value);
        return this;
    }

    @Override
    public ElementParser<T> onInt(int value) {
        logger.trace("onInt({})", value);
        return this;
    }

    @Override
    public ElementParser<T> onLong(long value) {
        logger.trace("onLong({})", value);
        return this;
    }

    @Override
    public ElementParser<T> onMapHeader(int numberOfElements) {
        logger.trace("onMapHeader(numberOfElements={})", numberOfElements);
        return this;
    }

    @Override
    public ElementParser<T> onNull() {
        logger.trace("onNull()");
        return this;
    }

    @Override
    public ElementParser<T> onRaw(byte[] value) {
        if (logger.isTraceEnabled()) {
            logger.trace("onRaw({})", Arrays.toString(value));
        }
        return this;
    }

    @Override
    public ElementParser<T> onString(String value) {
        logger.trace("onString({})", value);
        return this;
    }
}
