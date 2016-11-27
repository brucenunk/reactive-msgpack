package jamesl.reactive.msgpack;

/**
 * @author jamesl
 * @since 1.0
 */
public class UnsupportedWireFormatException extends RuntimeException {
    public UnsupportedWireFormatException(byte value) {
        super("unrecognised wire format - value = " + value);
    }
}
