import com.kafka.ConsumerRegisterMessage;
import com.kafka.Message;
import com.kafka.MessageType;
import com.kafka.ProducerRegisterMessage;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.junit.jupiter.api.Assertions.*;

class MessageTest {

    @Test
    void shouldWriteAndReadEcho() throws Exception {

        Message original = Message.builder()
                .echo("hello")
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertEquals("hello", decoded.getEcho());
        assertNull(decoded.getREcho());
    }

    @Test
    void shouldWriteAndReadResponseEcho() throws Exception {

        Message original = Message.builder()
                .rEcho("ok")
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertEquals("ok", decoded.getREcho());
        assertNull(decoded.getEcho());
    }

    @Test
    void shouldWriteAndReadProducerRegister() throws Exception {

        ProducerRegisterMessage reg =
                new ProducerRegisterMessage(8080, 10);

        Message original = Message.builder()
                .pReg(reg)
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertEquals(8080, decoded.getPReg().getPort());
        assertEquals(10, decoded.getPReg().getTopicId());
    }

    @Test
    void shouldWriteAndReadProducerRegisterResponse() throws Exception {

        Message original = Message.builder()
                .rPReg((byte) 1)
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertEquals((byte) 1, decoded.getRPReg());
    }

    @Test
    void shouldWriteAndReadProducerConsumerMessage() throws Exception {

        byte[] pcm = {1, 2, 3, 4, 5};

        Message original = Message.builder()
                .pcm(pcm)
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertArrayEquals(pcm, decoded.getPcm());
    }

    @Test
    void shouldWriteAndReadProducerConsumerResponse() throws Exception {

        Message original = Message.builder()
                .rPcm((byte) 0)
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertEquals((byte) 0, decoded.getRPcm());
    }

    @Test
    void shouldWriteAndReadConsumerRegister() throws Exception {

        ConsumerRegisterMessage reg =
                new ConsumerRegisterMessage(9000, 20);

        Message original = Message.builder()
                .cReg(reg)
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertEquals(9000, decoded.getCReg().getPort());
        assertEquals(20, decoded.getCReg().getTopicId());
    }

    @Test
    void shouldThrowForUnknownMessageType() {

        byte[] raw = {
                2,
                99,
                1
        };

        assertThrows(
                IllegalArgumentException.class,
                () -> Message.readFrom(
                        new DataInputStream(
                                new ByteArrayInputStream(raw)
                        )
                )
        );
    }

    @Test
    void shouldSerializeEchoWhenMultipleFieldsArePresent() throws Exception {

        Message message = Message.builder()
                .echo("hello")
                .rEcho("ignored")
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        message.writeTo(new DataOutputStream(baos));

        byte[] bytes = baos.toByteArray();

        assertEquals(MessageType.ECHO, bytes[1]);

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(bytes)
                )
        );

        assertEquals("hello", decoded.getEcho());
    }

    @Test
    void shouldSupportEmptyEcho() throws Exception {

        Message original = Message.builder()
                .echo("")
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertEquals("", decoded.getEcho());
    }

    @Test
    void shouldSupportUtf8Echo() throws Exception {

        String text = "Xin chào 😄";

        Message original = Message.builder()
                .echo(text)
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertEquals(text, decoded.getEcho());
    }

    @Test
    void shouldSupportEmptyPcm() throws Exception {

        Message original = Message.builder()
                .pcm(new byte[0])
                .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        original.writeTo(new DataOutputStream(baos));

        Message decoded = Message.readFrom(
                new DataInputStream(
                        new ByteArrayInputStream(baos.toByteArray())
                )
        );

        assertArrayEquals(new byte[0], decoded.getPcm());
    }

    @Test
    void shouldThrowWhenPayloadLengthIsZero() {

        // length = 0 -> payload rỗng
        byte[] raw = {0};

        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> Message.readFrom(
                        new DataInputStream(
                                new ByteArrayInputStream(raw)
                        )
                )
        );
    }

    @Test
    void shouldThrowWhenProducerRegisterPayloadTooShort() {

        // Length = 3
        // type = P_REG
        // chỉ có 2 bytes dữ liệu thay vì 4
        byte[] raw = {
                3,
                MessageType.P_REG,
                0,
                1
        };

        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> Message.readFrom(
                        new DataInputStream(
                                new ByteArrayInputStream(raw)
                        )
                )
        );
    }

    @Test
    void shouldThrowWhenConsumerRegisterPayloadTooShort() {

        byte[] raw = {
                3,
                MessageType.C_REG,
                0,
                1
        };

        assertThrows(
                ArrayIndexOutOfBoundsException.class,
                () -> Message.readFrom(
                        new DataInputStream(
                                new ByteArrayInputStream(raw)
                        )
                )
        );
    }

    @Test
    void shouldFailRoundTripWhenPayloadIsLargerThan255Bytes() throws Exception {

//        byte[] data = new byte[300];
//
//        for (int i = 0; i < data.length; i++) {
//            data[i] = (byte) i;
//        }
//
//        Message original = Message.builder()
//                .pcm(data)
//                .build();
//
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//        original.writeTo(new DataOutputStream(baos));
//
//        assertThrows(
//                Exception.class,
//                () -> Message.readFrom(
//                        new DataInputStream(
//                                new ByteArrayInputStream(baos.toByteArray())
//                        )
//                )
//        );
    }
}