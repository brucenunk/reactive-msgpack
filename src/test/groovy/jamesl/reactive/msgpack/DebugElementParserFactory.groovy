package jamesl.reactive.msgpack

import java.util.function.Consumer

/**
 * @author jamesl
 * @since 1.0
 */
class DebugElementParserFactory implements ElementParserFactory<String> {
    @Override
    ElementParser<String> firstElementParser(Consumer<String> consumer) {
        return new DebugElementParser(consumer)
    }

    /**
     *
     */
    static class DebugElementParser implements ElementParser<String> {
        Consumer<String> downstream

        DebugElementParser(Consumer<String> downstream) {
            this.downstream = downstream
        }

        @Override
        boolean isRoutingNullsViaOnStringEnabled() {
            false
        }

        @Override
        ElementParser<String> onArrayHeader(int numberOfElements) {
            dispatchEvent("onArrayHeader|${numberOfElements}")
        }

        @Override
        ElementParser<String> onBigInteger(BigInteger value) {
            dispatchEvent("onBigInteger|${value}")
        }

        @Override
        ElementParser<String> onBoolean(boolean value) {
            dispatchEvent("onBoolean|${value}")
        }

        @Override
        ElementParser<String> onByte(byte value) {
            dispatchEvent("onByte|${value}")
        }

        @Override
        ElementParser<String> onDouble(double value) {
            dispatchEvent("onDouble|${value}")
        }

        @Override
        ElementParser<String> onInt(int value) {
            dispatchEvent("onInt|${value}")
        }

        @Override
        ElementParser<String> onLong(long value) {
            dispatchEvent("onLong|${value}")
        }

        @Override
        ElementParser<String> onMapHeader(int numberOfElements) {
            dispatchEvent("onMapHeader|${numberOfElements}")
        }

        @Override
        ElementParser<String> onNull() {
            dispatchEvent("onNull")
        }

        @Override
        ElementParser<String> onRaw(byte[] value) {
            def s = Arrays.toString(value)
            dispatchEvent("onRaw|${s}")
        }

        @Override
        ElementParser<String> onString(String value) {
            dispatchEvent("onString|${value}")
        }

        ElementParser<String> dispatchEvent(String s) {
            downstream.accept(s)
            this
        }
    }
}
