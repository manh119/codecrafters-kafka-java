package kafka.record;

import protocol.io.DataInput;

import java.util.List;
import java.util.UUID;

public interface Record {

    public record FeatureLevel(
            String name,
            short featureLevel
    ) implements Record {

        public static FeatureLevel deserialize(DataInput input) {
            final var name = input.readCompactString();
            final var featureLevel = input.readSignedShort();

            input.skipEmptyTaggedFieldArray();

            return new FeatureLevel(
                    name,
                    featureLevel
            );
        }
    }

    public record Topic(
            String name,
            UUID id
    ) implements Record {

        public static Topic deserialize(DataInput input) {
            final var name = input.readCompactString();
            final var id = input.readUuid();

            input.skipEmptyTaggedFieldArray();

            return new Topic(
                    name,
                    id
            );
        }
    }

    public record Partition(
            int id,
            UUID topicId,
            List<Integer> replicas,
            List<Integer> inSyncReplicas,
            List<Integer> removingReplicas,
            List<Integer> addingReplicas,
            int leader,
            int leaderEpoch,
            int partitionEpoch,
            List<UUID> directories
    ) implements Record {

        public static Partition deserialize(DataInput input) {
            final var id = input.readSignedInt();
            final var topicId = input.readUuid();
            final var replicas = input.readCompactArray(DataInput::readSignedInt);
            final var inSyncReplicas = input.readCompactArray(DataInput::readSignedInt);
            final var removingReplicas = input.readCompactArray(DataInput::readSignedInt);
            final var addingReplicas = input.readCompactArray(DataInput::readSignedInt);
            final var leader = input.readSignedInt();
            final var leaderEpoch = input.readSignedInt();
            final var partitionEpoch = input.readSignedInt();
            final var directories = input.readCompactArray(DataInput::readUuid);

            input.skipEmptyTaggedFieldArray();

            return new Partition(
                    id,
                    topicId,
                    replicas,
                    inSyncReplicas,
                    removingReplicas,
                    addingReplicas,
                    leader,
                    leaderEpoch,
                    partitionEpoch,
                    directories
            );
        }
    }

    @SuppressWarnings("unused")
    public static Record deserialize(DataInput input) {
        final var length = input.readUnsignedVarint();
        final var attributes = input.readSignedByte();
        final var timestampDelta = input.readUnsignedVarint();
        final var offsetDelta = input.readUnsignedVarint();

        final var key = input.readCompactString();
        final var valueLength = input.readUnsignedVarint();

        final var recordFrameVersion = input.readSignedByte();
        final var recordType = input.readSignedByte();
        final var recordVersion = input.readSignedByte();

        final var record = switch (recordType) {
            case 2 -> Topic.deserialize(input);
            case 3 -> Partition.deserialize(input);
            case 12 -> FeatureLevel.deserialize(input);
            default -> throw new IllegalArgumentException("unknown record type: %s".formatted(recordType));
        };

        final var headers = input.readCompactDict(
                DataInput::readCompactString,
                DataInput::readCompactBytes
        );

        return record;
    }
}
