package message.describetopic;

import protocol.ErrorCode;
import protocol.ResponseBody;
import protocol.io.DataOutput;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public record DescribeTopicPartitionsResponseV0(
        Duration throttleTime,
        List<Topic> topics,
        DescribeTopicPartitionCursorV0 nextCursor
    ) implements ResponseBody {

    @Override
    public void serialize(DataOutput output) {
        output.writeInt((int) throttleTime.toMillis());
        output.writeCompactArray(topics, Topic::serialize);
        DescribeTopicPartitionCursorV0.serialize(nextCursor, output);

        output.skipEmptyTaggedFieldArray();
    }

    public record Topic(
            ErrorCode errorCode,
            String name,
            UUID topicId,
            boolean isInternal,
            List<Partition> partitions,
            int topicAuthorizedOperations
    ) {

        public void serialize(DataOutput output) {
            output.writeShort(errorCode.value());
            output.writeCompactString(name);
            output.writeUuid(topicId);
            output.writeBoolean(isInternal);
            output.writeCompactArray(partitions, Partition::serialize);
            output.writeInt(topicAuthorizedOperations);

            output.skipEmptyTaggedFieldArray();
        }

        public record Partition(
                ErrorCode errorCode,
                int partitionIndex,
                int leaderId,
                int leaderEpoch,
                List<Integer> replicaNodes,
                List<Integer> inSyncReplicaNodes,
                List<Integer> eligibleLeaderReplicas,
                List<Integer> lastKnownElr,
                List<Integer> offlineReplicas
        ) {

            public void serialize(DataOutput output) {
                output.writeShort(errorCode.value());
                output.writeInt(partitionIndex);
                output.writeInt(leaderId);
                output.writeInt(leaderEpoch);
                output.writeCompactIntArray(replicaNodes);
                output.writeCompactIntArray(inSyncReplicaNodes);
                output.writeCompactIntArray(eligibleLeaderReplicas);
                output.writeCompactIntArray(lastKnownElr);
                output.writeCompactIntArray(offlineReplicas);

                output.skipEmptyTaggedFieldArray();
            }
        }
    }
}
