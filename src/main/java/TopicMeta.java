import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

class TopicMeta {
    UUID topicId;
    Map<Integer, PartitionMeta> partitions = new HashMap<>();
}