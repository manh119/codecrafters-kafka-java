package protocol;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

@Getter
@RequiredArgsConstructor
@Accessors(fluent = true)
public enum ErrorCode {

    NONE(0),
    UNKNOWN_SERVER_ERROR(-1),
    UNKNOWN_TOPIC(3),
    UNSUPPORTED_VERSION(35),
    UNKNOWN_TOPIC_ID(100);

    private final short value;

    private ErrorCode(int value) {
        this((short) value);
    }
}
