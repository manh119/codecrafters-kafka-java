package message.produce;

import protocol.ErrorCode;
import protocol.ResponseBody;
import protocol.io.DataOutput;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public record ProduceResponseV11(
        Duration throttleTime,
        ErrorCode errorCode,
        int sessionId,
        List<Response> responses
) implements ResponseBody {

    @Override
    public void serialize(DataOutput output) {
        output.writeInt((int) throttleTime.toMillis());
        output.writeShort(errorCode().value());

        if (!ErrorCode.NONE.equals(errorCode)) return;

        output.writeInt(sessionId);
        output.writeCompactArray(responses, Response::serialize);

        output.skipEmptyTaggedFieldArray();
    }

    public record Response(
            UUID topicId,
            List<Partition> partitions
    ) {

        public void serialize(DataOutput output) {
            output.writeUuid(topicId);
            output.writeCompactArray(partitions, Partition::serialize);

            output.skipEmptyTaggedFieldArray();
        }

        public record Partition(
                int partitionIndex,
                ErrorCode errorCode,
                long highWatermark,
                long lastStableOffset,
                long logStartOffset,
                List<AbortedTransaction> abortedTransactions,
                int preferredReadReplica,
                byte[] records
        ) {

            public void serialize(DataOutput output) {
                output.writeInt(partitionIndex);
                output.writeShort(errorCode.value());
                output.writeLong(highWatermark);
                output.writeLong(lastStableOffset);
                output.writeLong(logStartOffset);
                output.writeCompactArray(abortedTransactions, AbortedTransaction::serialize);
                output.writeInt(preferredReadReplica);
                output.writeCompactBytes(records);

                output.skipEmptyTaggedFieldArray();
            }

            public record AbortedTransaction(
                    long producerId,
                    long firstOffset
            ) {

                public void serialize(DataOutput output) {
                    output.writeLong(producerId);
                    output.writeLong(firstOffset);

                    output.skipEmptyTaggedFieldArray();
                }
            }
        }
    }
}
