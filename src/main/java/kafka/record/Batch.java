package kafka.record;

import protocol.io.DataByteBuffer;
import protocol.io.DataInput;

import java.util.List;

public record Batch(
        long baseOffset,
        int partitionLeaderEpoch,
        byte magic,
        int crc,
        short attributes,
        int lastOffsetDelta,
        long baseTimestamp,
        long maxTimestamp,
        long producerId,
        short producerEpoch,
        int baseSequence,
        List<Record> records
) {

    public static Batch deserialize(DataInput input) {
        final var baseOffset = input.readSignedLong();

        input = new DataByteBuffer(input.readBytes());
        final var partitionLeaderEpoch = input.readSignedInt();
        final var magic = input.readSignedByte();
        final var crc = input.readSignedInt();
        final var attributes = input.readSignedShort();
        final var lastOffsetDelta = input.readSignedInt();
        final var baseTimestamp = input.readSignedLong();
        final var maxTimestamp = input.readSignedLong();
        final var producerId = input.readSignedLong();
        final var producerEpoch = input.readSignedShort();
        final var baseSequence = input.readSignedInt();

        final var records = input.readArray(Record::deserialize);

        return new Batch(
                baseOffset,
                partitionLeaderEpoch,
                magic,
                crc,
                attributes,
                lastOffsetDelta,
                baseTimestamp,
                maxTimestamp,
                producerId,
                producerEpoch,
                baseSequence,
                records
        );
    }
}
