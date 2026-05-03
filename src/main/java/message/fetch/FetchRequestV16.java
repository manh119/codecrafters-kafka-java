package message.fetch;

import protocol.RequestApi;
import protocol.RequestBody;
import protocol.io.DataInput;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public record FetchRequestV16(
        Duration maxWait,
        int minBytes,
        int maxBytes,
        byte isolationLevel,
        int sessionId,
        int sessionEpoch,
        List<Topic> topics,
        List<ForgottenTopicsData> forgottenTopicsData,
        String rackId
) implements RequestBody {

    public static final RequestApi API = RequestApi.of(1, 16);

    public static FetchRequestV16 deserialize(DataInput input) {
        final var maxWait = Duration.ofMillis(input.readSignedInt());
        final var minBytes = input.readSignedInt();
        final var maxBytes = input.readSignedInt();
        final var isolationLevel = input.readSignedByte();
        final var sessionId = input.readSignedInt();
        final var sessionEpoch = input.readSignedInt();
        final var topics = input.readCompactArray(Topic::deserialize);
        final var forgottenTopicsData = input.readCompactArray(ForgottenTopicsData::deserialize);
        final var rackId = input.readCompactString();

        input.skipEmptyTaggedFieldArray();

        return new FetchRequestV16(
                maxWait,
                minBytes,
                maxBytes,
                isolationLevel,
                sessionId,
                sessionEpoch,
                topics,
                forgottenTopicsData,
                rackId
        );
    }

    public record Topic(
            UUID topicId,
            List<Partition> partitions
    ) {

        public static Topic deserialize(DataInput input) {
            final var topicId = input.readUuid();
            final var partitions = input.readCompactArray(Partition::deserialize);

            input.skipEmptyTaggedFieldArray();

            return new Topic(
                    topicId,
                    partitions
            );
        }

        public record Partition(
                int partition,
                int currentLeaderEpoch,
                long fetchOffset,
                int lastFetchedEpoch,
                long logStartOffset,
                int partitionMaxBytes
        ) {

            public static Partition deserialize(DataInput input) {
                final var partition = input.readSignedInt();
                final var currentLeaderEpoch = input.readSignedInt();
                final var fetchOffset = input.readSignedLong();
                final var lastFetchedEpoch = input.readSignedInt();
                final var logStartOffset = input.readSignedLong();
                final var partitionMaxBytes = input.readSignedInt();

                input.skipEmptyTaggedFieldArray();

                return new Partition(
                        partition,
                        currentLeaderEpoch,
                        fetchOffset,
                        lastFetchedEpoch,
                        logStartOffset,
                        partitionMaxBytes
                );
            }
        }
    }

    public record ForgottenTopicsData(
            UUID topicId,
            List<Integer> partitions
    ) {

        public static ForgottenTopicsData deserialize(DataInput input) {
            final var topicId = input.readUuid();
            final var partitions = input.readCompactArray(DataInput::readSignedInt);

            input.skipEmptyTaggedFieldArray();

            return new ForgottenTopicsData(
                    topicId,
                    partitions
            );
        }
    }
}
