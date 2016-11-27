package jamesl.reactive.msgpack;

import java.util.function.Consumer;

/**
 * @param <T>
 * @author jamesl
 * @since 1.0
 */
public interface ElementParserFactory<T> {
    ElementParser<T> firstElementParser(Consumer<T> consumer);
}
