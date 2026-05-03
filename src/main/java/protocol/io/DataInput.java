package protocol.io;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Function;

public interface DataInput {

    ByteBuffer readNBytes(int n);

    byte peekByte();

    byte readSignedByte();

    short readSignedShort();

    int readSignedInt();

    long readSignedLong();

    default UUID readUuid() {
        return new UUID(readSignedLong(), readSignedLong());
    }

    default long readUnsignedVarint() {
        return VarInt.readLong(this);
    }

    default ByteBuffer readBytes() {
        final var length = readSignedInt();

        if (length == -1) return null;

        return readNBytes(length);
    }

    default ByteBuffer readCompactBytes() {
        final var length = readUnsignedVarint();

        if (length == 0) return null;

        return readNBytes((int) length - 1);
    }

    default String readString() {
        final var length = readSignedShort();

        if (length == -1) return null;

        return asString(readNBytes(length));
    }

    default String readCompactString() {
        return asString(readCompactBytes());
    }

    default <T> List<T> readArray(Function<DataInput, T> deserializer) {
        final var length = readSignedInt();

        if (length == -1) return null;

        final var items = new ArrayList<T>(length);
        for (var index = 0; index < length; index++) {
            items.add(deserializer.apply(this));
        }

        return items;
    }

    default <T> List<T> readCompactArray(Function<DataInput, T> deserializer) {
        var length = readUnsignedVarint();

        if (length == 0) return null;

        length--;
        final var items = new ArrayList<T>((int) length);

        for (var index = 0; index < length; index++) {
            items.add(deserializer.apply(this));
        }

        return items;
    }

    default <K, V> Map<K, V> readCompactDict(Function<DataInput, K> keyDeserializer, Function<DataInput, V> valueDeserializer) {
        var length = readUnsignedVarint();

        if (length == 0) return null;

        length--;
        final var dict = HashMap.<K, V>newHashMap((int) length);

        for (var index = 0; index < length; index++) {
            final var key = keyDeserializer.apply(this);
            final var value = valueDeserializer.apply(this);

            dict.put(key, value);
        }

        return dict;
    }

    default void skipEmptyTaggedFieldArray() {
        readUnsignedVarint();
    }

    private String asString(ByteBuffer buffer) {
        if (buffer == null) return null;

        return new String(buffer.array(), buffer.arrayOffset(), buffer.limit(), StandardCharsets.UTF_8);
    }
}
