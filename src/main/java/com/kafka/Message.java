package com.kafka;

import lombok.Builder;
import lombok.Getter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Mirrors Go's com.kafka.Message struct.
 *
 * Wire format (from com.kafka.message.go):
 *   byte[0]  : total payload length  (stream[0] = size)
 *   byte[1]  : com.kafka.message type
 *   byte[2…] : payload
 *
 * Each field is exclusive — only one is non-null per com.kafka.message instance.
 */
@Getter
@Builder
public class Message {

    // Request fields
    private final String                  echo;
    private final ProducerRegisterMessage pReg;
    private final byte[]                  pcm;

    // Response fields
    private final String rEcho;
    private final Byte   rPReg;
    private final Byte   rPcm;

    // ------------------------------------------------------------------
    // Read
    // ------------------------------------------------------------------

    /**
     * Reads exactly one com.kafka.Message from the stream (mirrors readMessageFromStream + parseMessage).
     *
     * Protocol:
     *   1 byte  – payload length (N)
     *   N bytes – [type_byte | data...]
     */
    public static Message readFrom(DataInputStream in) throws IOException {
        int length = in.readUnsignedByte(); // blocks until data arrives
        byte[] payload = in.readNBytes(length);
        return parse(payload);
    }

    private static Message parse(byte[] payload) {
        byte type = payload[0];
        byte[] data = new byte[payload.length - 1];
        System.arraycopy(payload, 1, data, 0, data.length);

        return switch (type) {
            case MessageType.ECHO   -> Message.builder().echo(new String(data, StandardCharsets.UTF_8)).build();
            case MessageType.R_ECHO -> Message.builder().rEcho(new String(data, StandardCharsets.UTF_8)).build();
            case MessageType.P_REG  -> Message.builder().pReg(ProducerRegisterMessage.fromBytes(data)).build();
            case MessageType.R_P_REG -> Message.builder().rPReg(data[0]).build();
            case MessageType.PCM    -> Message.builder().pcm(data).build();
            case MessageType.R_PCM  -> Message.builder().rPcm(data[0]).build();
            default -> throw new IllegalArgumentException("Unknown com.kafka.message type: " + type);
        };
    }

    // ------------------------------------------------------------------
    // Write
    // ------------------------------------------------------------------

    /**
     * Writes this com.kafka.Message to the stream (mirrors writeMessageToStream in Go).
     *
     * Format per field:
     *   [length_byte][type_byte][data...]
     */
    public void writeTo(DataOutputStream out) throws IOException {
        if (echo != null) {
            writePayload(out, MessageType.ECHO, echo.getBytes(StandardCharsets.UTF_8));
        } else if (rEcho != null) {
            writePayload(out, MessageType.R_ECHO, rEcho.getBytes(StandardCharsets.UTF_8));
        } else if (pReg != null) {
            writePayload(out, MessageType.P_REG, pReg.toBytes());
        } else if (rPReg != null) {
            writePayload(out, MessageType.R_P_REG, new byte[]{rPReg});
        } else if (pcm != null) {
            writePayload(out, MessageType.PCM, pcm);
        } else if (rPcm != null) {
            writePayload(out, MessageType.R_PCM, new byte[]{rPcm});
        }
        out.flush();
    }

    /**
     * Writes: [length][type][data].
     * length = data.length + 1 (for the type byte) — matches Go's len(data)+1.
     */
    private void writePayload(DataOutputStream out, byte type, byte[] data) throws IOException {
        out.writeByte(data.length + 1); // length byte
        out.writeByte(type);
        out.write(data);
    }
}
