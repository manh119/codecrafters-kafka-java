import java.util.ArrayList;
import java.util.List;

class PartitionMeta {
    int partitionId;
    int leaderId;
    List<Integer> replicas = new ArrayList<>();
}