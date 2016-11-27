package jamesl.reactive.msgpack

import java.util.function.Consumer

/**
 * @author jamesl
 * @since 1.0
 */
class MessageElementParserFactory implements ElementParserFactory<Message> {
    @Override
    ElementParser<Message> firstElementParser(Consumer<Message> consumer) {
        return new NameParser(consumer)
    }

    /**
     *
     */
    static class NameParser extends DefaultElementParser<Message> {
        Consumer<Message> consumer

        NameParser(Consumer<Message> consumer) {
            this.consumer = consumer
        }

        @Override
        ElementParser<Message> onString(String value) {
            new InterestsParser(consumer, new Message(name: value))
        }
    }

    /**
     *
     */
    static class InterestsParser extends DefaultElementParser<Message> {
        Consumer<Message> consumer
        Message message
        int numberOfElements

        InterestsParser(Consumer<Message> consumer, Message message) {
            this.consumer = consumer
            this.message = message
        }

        @Override
        ElementParser<Message> onArrayHeader(int numberOfElements) {
            this.numberOfElements = numberOfElements
            this
        }

        @Override
        ElementParser<Message> onString(String value) {
            message.interests << value
            numberOfElements--

            if (numberOfElements > 0) {
                this
            } else {
                new RatingsParser(consumer, message)
            }
        }
    }

    /**
     *
     */
    static class RatingsParser extends DefaultElementParser<Message> {
        Consumer<Message> consumer
        Message message
        String name
        int numberOfElements

        RatingsParser(Consumer<Message> consumer, Message message) {
            this.consumer = consumer
            this.message = message
        }

        @Override
        ElementParser<Message> onInt(int value) {
            message.ratings[name] = value
            numberOfElements--

            if (numberOfElements > 0) {
                this
            } else {
                new ChecksumParser(consumer, message)
            }
        }

        @Override
        ElementParser<Message> onMapHeader(int numberOfElements) {
            this.numberOfElements = numberOfElements
            this
        }

        @Override
        ElementParser<Message> onString(String value) {
            this.name = value
            this
        }
    }

    /**
     *
     */
    static class ChecksumParser extends DefaultElementParser {
        Consumer<Message> consumer
        Message message

        ChecksumParser(Consumer<Message> consumer, Message message) {
            this.consumer = consumer
            this.message = message
        }

        @Override
        ElementParser onLong(long value) {
            message.checksum = value
            consumer.accept(message)
            super.onLong(value)
        }
    }
}
