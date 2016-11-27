package jamesl.reactive.msgpack

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

import java.nio.ByteBuffer

/**
 * @author jamesl
 * @since 1.0
 */
@EqualsAndHashCode
@ToString(includeNames = true)
class Message {
    long checksum
    String name
    List<String> interests = []
    Map<String, Integer> ratings = [:]

    ByteBuffer msgpack() {
        ByteBuffer buffer = ByteBuffer.allocate(200)
        pack(name, buffer)

        // JL array16.
        buffer.put(0xdc as byte).putShort(interests.size() as short)
        interests.forEach { x ->
            pack(x, buffer)
        }

        // JL map16.
        buffer.put(0xde as byte).putShort(ratings.size() as short)
        ratings.forEach { k, v ->
            pack(k, buffer)
            packInt(v, buffer)
        }

        buffer.put(0xd3 as byte).putLong(checksum)
        buffer.flip()
        buffer
    }

    /**
     * Packs {@code s} as a msgpack "str8" element.
     * @param buffer
     * @param s
     */
    private static void pack(String s, ByteBuffer buffer) {
        def sb = s.bytes
        buffer.put(0xd9 as byte).put(sb.length as byte).put(sb)
    }

    /**
     * Packs {@code v} as a msgpack "int
     * @param buffer
     * @param v
     */
    private static void packInt(int v, ByteBuffer buffer) {
        buffer.put(0xd2 as byte).putInt(v)
    }
}
