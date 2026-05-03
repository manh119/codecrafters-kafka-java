package protocol;

import lombok.Getter;
import lombok.experimental.Accessors;
import message.apiversions.ApiVersionRequestV4;
import message.describetopic.DescribeTopicPartitionsRequestV0;
import message.fetch.FetchRequestV16;
import protocol.io.DataByteBuffer;
import protocol.io.DataInput;
import protocol.io.DataOutput;
import protocol.io.DataOutputStream;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Getter
@Accessors(fluent = true)
public class ExchangeMapper {

    private final Map<RequestApi, Function<DataInput, ? extends RequestBody>> deserializers = new HashMap<>();

    public ExchangeMapper() {
        deserializers.put(ApiVersionRequestV4.API, ApiVersionRequestV4::deserialize);
        deserializers.put(DescribeTopicPartitionsRequestV0.API, DescribeTopicPartitionsRequestV0::deserialize);
        deserializers.put(FetchRequestV16.API, FetchRequestV16::deserialize);
    }

    public Request receiveRequest(DataInput input) {
        final var messageSize = input.readSignedInt();
        final var buffer = input.readNBytes(messageSize);
        input = new DataByteBuffer(buffer);

        final var header = Header.V2.deserialize(input);

        final var deserializer = getDeserializer(header);
        final var body = deserializer.apply(input);

        return new Request(header, body);
    }

    public void sendResponse(DataOutput output, Response response) {
        final var byteOutputStream = new ByteArrayOutputStream();

        final var temporaryOutput = new DataOutputStream(byteOutputStream);
        response.serialize(temporaryOutput);

        final var bytes = byteOutputStream.toByteArray();

        output.writeInt(bytes.length);
        output.writeRawBytes(bytes);
    }

    private Function<DataInput, ? extends RequestBody> getDeserializer(Header.V2 header) {
        final var deserializer = deserializers.get(header.requestApi());

        if (deserializer == null) throw new ProtocolException(ErrorCode.UNSUPPORTED_VERSION, header.correlationId());

        return deserializer;
    }

    public void sendErrorResponse(DataOutput output, int correlationId, ErrorCode errorCode) {
        output.writeInt(4 + 2);
        output.writeInt(correlationId);
        output.writeShort(errorCode.value());
    }

    public List<RequestApi> requestApis() {
        return new ArrayList<>(deserializers.keySet());
    }
}
