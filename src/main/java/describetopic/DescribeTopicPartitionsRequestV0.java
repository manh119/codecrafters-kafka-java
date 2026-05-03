package describetopic;

import protocol.RequestApi;
import protocol.RequestBody;
import protocol.io.DataInput;

import java.util.List;

public record DescribeTopicPartitionsRequestV0(List<Topic> topics,
                                               int responsePartitionLimit,
                                               DescribeTopicPartitionCursorV0 nextCursor) implements RequestBody {

    public static final RequestApi API = RequestApi.of(75, 0);

    public static DescribeTopicPartitionsRequestV0 deserialize(DataInput input) {
        final var topics = input.readCompactArray(Topic::deserialize);
        final var responsePartitionLimit = input.readSignedInt();
        final var nextCursor = DescribeTopicPartitionCursorV0.deserialize(input);

        return new DescribeTopicPartitionsRequestV0(topics, responsePartitionLimit, nextCursor);
    }

    public record Topic(String name) {

        public static Topic deserialize(DataInput input) {
            final var name = input.readCompactString();

            input.skipEmptyTaggedFieldArray();

            return new Topic(name);
        }
    }
}
