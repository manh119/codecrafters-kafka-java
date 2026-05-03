package message.apiversions;

import protocol.RequestApi;
import protocol.RequestBody;
import protocol.io.DataInput;

public record ApiVersionRequestV4(ClientSoftware clientSoftware) implements RequestBody {

    public static final RequestApi API = RequestApi.of(18, 4);

    public static ApiVersionRequestV4 deserialize(DataInput input) {
        final var clientSoftwareName = input.readCompactString();
        final var clientSoftwareVersion = input.readCompactString();

        input.skipEmptyTaggedFieldArray();

        return new ApiVersionRequestV4(new ClientSoftware(
                clientSoftwareName,
                clientSoftwareVersion
        ));
    }

    public record ClientSoftware(String name, String version) {}
}
