package jamesl.reactive.msgpack

import groovy.util.logging.Slf4j
import io.netty.buffer.Unpooled
import org.reactivestreams.Publisher
import reactor.core.publisher.Flux
import spock.lang.Specification
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.util.function.Function
import java.util.stream.Collectors

/**
 * @author jamesl
 * @since 1.0
 */
@Slf4j
class UnpackerSpec extends Specification {
    @Unroll
    def "parse single message element"(ByteBuffer buffer, String message) {
        ElementParserFactory<String> elementParserFactory = new DebugElementParserFactory()

        when:
        def s = unpack(elementParserFactory, buffer).collect(Collectors.joining()).block()

        then:
        s == message

        where:
        buffer                                                                                  | message
        gen { x -> x.put(20 as byte) }                                                          | "onByte|20"
        gen { x -> x.put(0xc0 as byte) }                                                        | "onNull"
        gen { x -> x.put(0xc2 as byte) }                                                        | "onBoolean|false"
        gen { x -> x.put(0xc3 as byte) }                                                        | "onBoolean|true"
        gen { x -> x.put(0xc4 as byte).put(2 as byte).put(10 as byte).put(12 as byte) }         | "onRaw|[10, 12]"
        gen { x -> x.put(0xc5 as byte).putShort(2 as short).put(20 as byte).put(22 as byte) }   | "onRaw|[20, 22]"
        gen { x -> x.put(0xc6 as byte).putInt(2).put(30 as byte).put(32 as byte) }              | "onRaw|[30, 32]"
        gen { x -> x.put(0xca as byte).putFloat(2.345f) }                                       | "onDouble|2.3450000286102295"
        gen { x -> x.put(0xcb as byte).putDouble(2.345) }                                       | "onDouble|2.345"
        gen { x -> x.put(0xcc as byte).put(20 as byte) }                                        | "onInt|20"
        gen { x -> x.put(0xcd as byte).putShort(20 as short) }                                  | "onInt|20"
        gen { x -> x.put(0xce as byte).putInt(20) }                                             | "onLong|20"
        gen { x -> x.put(0xcf as byte).putLong(20L) }                                           | "onBigInteger|20"
        gen { x -> x.put(0xd0 as byte).put(20 as byte) }                                        | "onByte|20"
        gen { x -> x.put(0xd1 as byte).putShort(20 as short) }                                  | "onInt|20"
        gen { x -> x.put(0xd2 as byte).putInt(20) }                                             | "onInt|20"
        gen { x -> x.put(0xd3 as byte).putLong(20L) }                                           | "onLong|20"
        gen { x -> x.put(0xd9 as byte).put("james".length() as byte).put("james".bytes) }       | "onString|james"
        gen { x -> x.put(0xda as byte).putShort("james".length() as short).put("james".bytes) } | "onString|james"
        gen { x -> x.put(0xdb as byte).putInt("james".length() as short).put("james".bytes) }   | "onString|james"
        gen { x -> x.put(0xdc as byte).putShort(20 as short) }                                  | "onArrayHeader|20"
        gen { x -> x.put(0xdd as byte).putInt(20) }                                             | "onArrayHeader|20"
        gen { x -> x.put(0xde as byte).putShort(20 as short) }                                  | "onMapHeader|20"
        gen { x -> x.put(0xdf as byte).putInt(20) }                                             | "onMapHeader|20"
    }

    def "parse message"() {
        ElementParserFactory<String> elementParserFactory = new DebugElementParserFactory()
        def message = new Message(checksum: 200, name: "james", interests: ["reactive", "msgpack"], ratings: [pizza: 100, beer: 100, gerkins: 0])

        when:
        def s = unpack(elementParserFactory, message.msgpack()).collect(Collectors.joining("\n")).block()

        then:
        s == """onString|james
onArrayHeader|2
onString|reactive
onString|msgpack
onMapHeader|3
onString|pizza
onInt|100
onString|beer
onInt|100
onString|gerkins
onInt|0
onLong|200""".toString()
    }

    def "parse message after random split (to simulate fragmentation on the wire)"() {
        ElementParserFactory<String> elementParserFactory = new DebugElementParserFactory()
        def message = new Message(checksum: 200, name: "james", interests: ["reactive", "msgpack"], ratings: [pizza: 100, beer: 100, gerkins: 0])

        when: "parse split buffers"
        def randomBuffers = randomSplit(4, message.msgpack())
        def s = unpack(elementParserFactory, randomBuffers).collect(Collectors.joining("\n")).block()

        then:
        s == """onString|james
onArrayHeader|2
onString|reactive
onString|msgpack
onMapHeader|3
onString|pizza
onInt|100
onString|beer
onInt|100
onString|gerkins
onInt|0
onLong|200""".toString()
    }

    def "parse message using stateful element parsers"() {
        ElementParserFactory<Message> elementParserFactory = new MessageElementParserFactory()
        def message = new Message(checksum: 200, name: "james", interests: ["reactive", "msgpack"], ratings: [pizza: 100, beer: 100, gerkins: 0])

        when: "parse split buffers"
        def m = unpack(elementParserFactory, randomSplit(4, message.msgpack())).blockFirst()

        then:
        m == message
    }

    /**
     * Generates a {@link ByteBuffer} and applies {@code mapper} to it.
     *
     * @param mapper
     * @return
     */
    def gen(Function<ByteBuffer, ByteBuffer> mapper) {
        mapper.andThen { x -> x.flip() }.apply(ByteBuffer.allocate(200))
    }

    /**
     * Splits {@code buffer} into a list of smaller buffers with max length of {@code max}.
     *
     * @param max
     * @param buffer
     * @return
     */
    List<ByteBuffer> randomSplit(int max, ByteBuffer buffer) {
        List<ByteBuffer> result = []

        def random = new Random()
        def tmp = Unpooled.wrappedBuffer(buffer)

        while (tmp.isReadable()) {
            def offset = random.nextInt(Math.min(max, tmp.readableBytes())) + 1
            def slice = tmp.slice(0, offset).touch("slice").retain()
            result << slice.nioBuffer()

            tmp = tmp.slice(offset, tmp.readableBytes() - offset).touch("slice2").retain()
        }

        log.debug("random split generated {} buffer(s)", result.size())
        result
    }

    /**
     * Unpackers {@code buffers}.
     *
     * @param buffers
     * @return
     */
    private <T> Flux<T> unpack(ElementParserFactory<T> elementParserFactory, ByteBuffer... buffers) {
        unpack(elementParserFactory, Flux.fromArray(buffers))
    }

    /**
     * Unpackers {@code buffers}.
     *
     * @param buffers
     * @return
     */
    private <T> Flux<T> unpack(ElementParserFactory<T> elementParserFactory, Iterable<ByteBuffer> buffers) {
        unpack(elementParserFactory, Flux.fromIterable(buffers))
    }

    /**
     * Unpackers {@code buffers}.
     *
     * @param buffers
     * @return
     */
    private <T> Flux<T> unpack(ElementParserFactory<T> elementParserFactory, Publisher<ByteBuffer> buffers) {
        def unpacker = new Unpacker<>(elementParserFactory)

        Flux.from(buffers)
        .flatMap { buffer ->
            unpacker.unpack(buffer)
        }
    }
}
