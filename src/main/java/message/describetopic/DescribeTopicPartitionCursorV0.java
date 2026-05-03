package message.describetopic;

import protocol.io.DataInput;
import protocol.io.DataOutput;

public record DescribeTopicPartitionCursorV0(String topicName, int partitionIndex) {

    public static DescribeTopicPartitionCursorV0 deserialize(DataInput input) {
        if (input.peekByte() == (byte) 0xff) return null;

        final var topicName = input.readCompactString();
        final var partitionIndex = input.readSignedInt();

        input.skipEmptyTaggedFieldArray();

        return new DescribeTopicPartitionCursorV0(
                topicName,
                partitionIndex
        );
    }

    public static void serialize(DescribeTopicPartitionCursorV0 cursor, DataOutput output) {
        if (cursor == null) {
            output.writeByte((byte) 0xff);
            return;
        }

        output.writeCompactString(cursor.topicName());
        output.writeInt(cursor.partitionIndex());

        output.skipEmptyTaggedFieldArray();
    }
}
