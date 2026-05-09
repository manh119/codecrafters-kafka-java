package com.kafka.apiversions;

import com.kafka.protocol.RequestApi;
import com.kafka.protocol.RequestBody;
import com.kafka.protocol.io.DataInput;

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
