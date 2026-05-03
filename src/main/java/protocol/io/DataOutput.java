package protocol.io;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

public interface DataOutput {

    void writeRawBytes(byte[] bytes);

    default void writeBoolean(boolean value) {
        writeByte((byte) (value ? 1 : 0));
    }

    void writeByte(byte value);

    void writeShort(short value);

    void writeInt(int value);

    void writeLong(long value);

    default void writeUuid(UUID value) {
        writeLong(value.getMostSignificantBits());
        writeLong(value.getLeastSignificantBits());
    }

    default void writeUnsignedVarint(long value) {
        VarInt.writeLong(value, this);
    }

    default void writeCompactBytes(byte[] value) {
        if (value == null) {
            writeUnsignedVarint(0);
            return;
        }

        writeUnsignedVarint(value.length + 1);
        writeRawBytes(value);
    }

    default void writeString(String value) {
        if (value == null) {
            writeShort((short) -1);
            return;
        }

        final var bytes = value.getBytes(StandardCharsets.UTF_8);

        writeShort((short) bytes.length);
        writeRawBytes(bytes);
    }

    default void writeCompactString(String value) {
        if (value == null) {
            writeUnsignedVarint(0);
            return;
        }

        final var bytes = value.getBytes(StandardCharsets.UTF_8);

        writeUnsignedVarint(bytes.length + 1);
        writeRawBytes(bytes);
    }

    default <T> void writeCompactArray(List<T> items, BiConsumer<T, DataOutput> serializer) {
        if (items == null) {
            writeUnsignedVarint(0);
            return;
        }

        writeUnsignedVarint(items.size() + 1l);

        for (final var item : items) {
            serializer.accept(item, this);
        }
    }

    default void writeCompactIntArray(List<Integer> items) {
        writeCompactArray(items, (value, output) -> output.writeInt(value));
    }

    default void skipEmptyTaggedFieldArray() {
        writeUnsignedVarint(0);
    }
}
