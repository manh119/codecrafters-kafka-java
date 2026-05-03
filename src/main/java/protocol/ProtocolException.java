package protocol;

import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
@SuppressWarnings("serial")
public class ProtocolException extends RuntimeException {

    private final ErrorCode code;
    private final int correlationId;

    public ProtocolException(ErrorCode code, int correlationId) {
        super(code.name());

        this.code = code;
        this.correlationId = correlationId;
    }

}
