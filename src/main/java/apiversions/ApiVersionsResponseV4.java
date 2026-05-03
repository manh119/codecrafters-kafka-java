package apiversions;

import protocol.ErrorCode;
import protocol.ResponseBody;
import protocol.io.DataOutput;

import java.time.Duration;
import java.util.List;

public record ApiVersionsResponseV4(List<Key> apiKeys, Duration throttleTime) implements ResponseBody {

    @Override
    public void serialize(DataOutput output) {
        output.writeShort(ErrorCode.NONE.value());
        output.writeCompactArray(apiKeys, Key::serialize);
        output.writeInt((int) throttleTime.toMillis());

        output.skipEmptyTaggedFieldArray();
    }

    public record Key(
            short apiKey,
            short minVersion,
            short maxVersion
    ) {

        public void serialize(DataOutput output) {
            output.writeShort(apiKey);
            output.writeShort(minVersion);
            output.writeShort(maxVersion);

            output.skipEmptyTaggedFieldArray();
        }
    }
}
