package com.kafka;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Wire format: [port_hi, port_lo, topicId_hi, topicId_lo] — 4 bytes total.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProducerRegisterMessage {

    private int port;    // uint16 in Go → int in Java (0–65535)
    private int topicId; // uint16 in Go → int in Java (0–65535)

    /**
     * Deserialise from the 4-byte wire payload
     */
    public static ProducerRegisterMessage fromBytes(byte[] data) {
        int port    = ((data[0] & 0xFF) << 8) | (data[1] & 0xFF);
        int topicId = ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
        return new ProducerRegisterMessage(port, topicId);
    }

    /**
     * Serialise to the 4-byte wire payload
     */
    public byte[] toBytes() {
        return new byte[]{
            (byte) (port >> 8),
            (byte) (port & 0xFF),
            (byte) (topicId >> 8),
            (byte) (topicId & 0xFF)
        };
    }

    @Override
    public String toString() {
        return "com.kafka.ProducerRegisterMessage{port=" + port + ", topicId=" + topicId + "}";
    }
}
