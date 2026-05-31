package com.kafka;

/**
 */
public final class MessageType {

    private MessageType() {}

    // Request types
    public static final byte ECHO  = 1;
    public static final byte P_REG = 2;  // producer register message
    public static final byte PCM   = 3;  // producer to consumer message
    public static final byte C_REG  = 4; // consumer register message request

    // Response types
    public static final byte R_ECHO  = 101;
    public static final byte R_P_REG = 102; // response of producer register message
    public static final byte R_PCM   = 103; // response producer to consumer message
    public static final byte R_C_REG   = 104; // response ack from consumer to broker
}
