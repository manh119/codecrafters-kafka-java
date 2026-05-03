package kafka;

import kafka.record.Batch;
import kafka.record.Record;
import lombok.SneakyThrows;
import protocol.io.DataInputStream;

import java.io.FileInputStream;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Kafka {

    private final String logsRoot;
    private final Map<UUID, Record.Topic> topicPerId;
    private final Map<String, Record.Topic> topicPerName;
    private final Map<UUID, List<Record.Partition>> partitionsPerTopicId;

    public Kafka(String logsRoot, List<Record.Topic> topics, List<Record.Partition> records) {
        this.logsRoot = logsRoot;

        this.topicPerId = topics
                .stream()
                .collect(Collectors.toMap(
                        Record.Topic::id,
                        Function.identity()
                ));

        this.topicPerName = topics
                .stream()
                .collect(Collectors.toMap(
                        Record.Topic::name,
                        Function.identity()
                ));

        this.partitionsPerTopicId = records
                .stream()
                .sorted(Comparator.comparing(Record.Partition::topicId))
                .collect(Collectors.groupingBy(
                        Record.Partition::topicId
                ));
    }

    public Record.Topic getTopic(UUID id) {
        return topicPerId.get(id);
    }

    public Record.Topic getTopic(String name) {
        return topicPerName.get(name);
    }

    public List<Record.Partition> getPartitions(UUID topicId) {
        return partitionsPerTopicId.getOrDefault(topicId, Collections.emptyList());
    }

    @SneakyThrows
    public byte[] getRecordData(String topicName, int partitionIndex) {
        final var path = logsRoot + "%s-%s/00000000000000000000.log".formatted(topicName, partitionIndex);

        try (final var fileInputStream = new FileInputStream(path)) {
            return fileInputStream.readAllBytes();
        }
    }

    @SneakyThrows
    public static Kafka load(String logsRoot) {
        final var path = logsRoot + "__cluster_metadata-0/00000000000000000000.log";

        try (final var fileInputStream = new FileInputStream(path)) {
            System.out.println(HexFormat.ofDelimiter("").formatHex(fileInputStream.readAllBytes()));
        }

        final var topics = new ArrayList<Record.Topic>();
        final var partitions = new ArrayList<Record.Partition>();

        try (final var fileInputStream = new FileInputStream(path)) {
            final var input = new DataInputStream(fileInputStream);

            while (fileInputStream.available() != 0) {
                final var batch = Batch.deserialize(input);
                System.out.println(batch);

                for (final var record : batch.records()) {
                    if (record instanceof Record.Topic topic) {
                        topics.add(topic);
                    } else if (record instanceof Record.Partition partition) {
                        partitions.add(partition);
                    }
                }
            }
        }

        return new Kafka(logsRoot, topics, partitions);
    }
}
