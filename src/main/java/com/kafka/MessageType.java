package com.kafka;

/**
 */
public final class MessageType {

    private MessageType() {}

    // Request types
    public static final byte ECHO  = 1;
    public static final byte P_REG = 2;
    public static final byte PCM   = 3;

    // Response types
    public static final byte R_ECHO  = 101;
    public static final byte R_P_REG = 102;
    public static final byte R_PCM   = 103;
}
