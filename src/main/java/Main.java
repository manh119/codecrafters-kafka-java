import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class Main {
    private static final int PORT = 9092;

    public static void main(String[] args) {
        try {
            var serverSocket = new ServerSocket(PORT);
            serverSocket.setReuseAddress(true); // Vẫn giữ dòng này

            // THÊM SHUTDOWN HOOK Ở ĐÂY
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (serverSocket != null && !serverSocket.isClosed()) {
                        serverSocket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }));

            System.out.println("Server started on port " + PORT);

            while (true) {
                try (Socket client = serverSocket.accept()) {
                    handleClient(client);
                } catch (IOException e) {
                    // Nếu socket bị đóng do shutdown hook, đừng in stacktrace rối mắt
                    if (!serverSocket.isClosed()) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void handleClient(Socket client) throws IOException {
        InputStream in = client.getInputStream();
        OutputStream out = client.getOutputStream();

        int size = ByteBuffer.wrap(in.readNBytes(4)).getInt();
        byte[] payload = in.readNBytes(size);
        System.out.println("Size :  " + size);
        System.out.println("Request payload (bytes): " + Arrays.toString(payload));
        System.out.println("Request payload (hex): " + toHex(payload));

        ByteBuffer req = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);

        ///  request header
        short apiKey = req.getShort();
        short apiVersion = req.getShort();
        int correlationId = req.getInt();
        short clientIdLength = req.getShort();
        req.position(req.position() + clientIdLength); // skip clientId content
        byte tagBuffer =  req.get();
        System.out.printf("apiKey=%d, apiVersion=%d, correlationId=%d%n", apiKey, apiVersion, correlationId);

        ///  request body
        String topic = null;
        if (apiKey == 75) {
            int topicsCount = req.get() - 1;
            topic = "";
            for (int i = 0; i < topicsCount; i++) {
                int len = req.get() - 1;

                byte[] bytes = new byte[len];
                req.get(bytes);

                topic = new String(bytes);
                System.out.println("Topic: " + topic);

                req.get(); // skip TAG_BUFFER
            }
        }

        /// Read log file
        /// extract
        /// - topicName
        /// - topicId (UUID)
        /// - partitionId
        /// - leaderId
        /// - replicas
        ByteBuffer buf = ByteBuffer.wrap(
                Files.readAllBytes(Paths.get(
                        "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
                ))
        );
        buf.order(ByteOrder.BIG_ENDIAN);
        System.out.println("Bytes from file " + Arrays.toString(buf.array()));
        while (buf.hasRemaining()) {

            if (buf.remaining() < 12) break;

            long baseOffset = buf.getLong();
            int batchLength = buf.getInt();
            System.out.println("Batch length: " + batchLength);

            int batchEnd = buf.position() + batchLength;

            if (buf.remaining() < 49) break;
            buf.position(buf.position() + 45); // skip header

            int recordCount = buf.getInt();
            System.out.println("Records count: " + recordCount);
            for (int i = 0; i < recordCount; i++) {

                int recordLength = buf.get();
                int recordStart = buf.position();

                buf.get(); // attributes
                readVarInt(buf); // timestampDelta
                readVarInt(buf); // offsetDelta

                // key
                int keyLen = readVarInt(buf);
                if (keyLen > 0) buf.position(buf.position() + keyLen);
                System.out.println("key len: " + keyLen);

                // value
                int valueLen = readVarInt(buf);
                System.out.println("value len: " + valueLen);
                if (valueLen > 0) {
                    byte[] value = new byte[valueLen];
                    buf.get(value);

                    parseValue(value);
                }

                buf.position(recordStart + recordLength);
            }

            buf.position(batchEnd);
        }

        /// build body response
        ByteBuffer body = ByteBuffer.allocate(1000).order(ByteOrder.BIG_ENDIAN);
        if (apiKey == 75) { // DescribeTopicPartitions
            body.putInt(0); // Throttle Time

            // topic array
            int actualSize = 1;
            body.put((byte) (actualSize + 1));
            for (int i = 0; i < actualSize; i++) {
                body.putShort((short) 3); // error code
                putCompactString(body, topic); // topic name
                byte[] bytes16 = new byte[16]; // topic id
                body.put(bytes16);
                body.put((byte) 0); // Is Internal
                body.put((byte) 0); // partition array
                body.putInt(0); //  Topic Authorized Operations
                body.put((byte) 0); // tag buffer
            }

            body.put((byte) -1); // next cursor
            body.put((byte) 0); // tag buffer
        }

        if (apiKey == 18) {
            int error_code = 0;
            if (apiVersion < 0 || apiVersion > 4) {
                error_code = 35; // UNSUPPORTED_VERSION
            }
            body.putShort((short) error_code);
            int actualSize = 2;
            body.put((byte) (actualSize + 1));

            // Entry: apiKey=18, min=0, max=4
            body.putShort((short) 18);   // api_key
            body.putShort((short) 0);    // min_version
            body.putShort((short) 4);    // max_version
            body.put((byte) 0); // tag buffer

            // Entry: apiKey=75, min=0, max=4
            body.putShort((short) 75);   // api_key
            body.putShort((short) 0);    // min_version
            body.putShort((short) 0);    // max_version
            body.put((byte) 0); // tag buffer

            body.putInt(0); // ThrottleTimeMs
            body.put((byte) 0); // tag buffer
        }

        ByteBuffer resp = ByteBuffer.allocate(4 + 1000).order(ByteOrder.BIG_ENDIAN);
        int bodyLength = body.position();
        int messageSize = 0;
        if (apiKey == 75) {
            messageSize = 5 + bodyLength; // correlationId + tag buffer + body
        } else {
            messageSize = 4 + bodyLength;
        }
        resp.putInt(messageSize);

        /// Response Header
        resp.putInt(correlationId);
        if (apiKey == 75) {
            resp.put((byte) 0); // tag buffer
        }

        /// Response Body
        resp.put(body.array(), 0, bodyLength);

        System.out.println("Message size: " + messageSize);
        System.out.println("Response payload (bytes): " + Arrays.toString(resp.array()));
        System.out.println("Response payload (hex): " + toHex(resp.array()));

        // Sau khi đã put hết dữ liệu vào resp
        resp.flip(); // Đặt limit = position, position = 0

        // Ghi byte theo đúng số lượng
        byte[] output = new byte[resp.remaining()];
        resp.get(output);
        out.write(output);
        out.flush();
        client.close();
    }

    private static String toHex(byte[] data) {
        StringBuilder sb = new StringBuilder();
        for (byte b : data) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString();
    }

    static void putCompactString(ByteBuffer buf, String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);

        // length = N + 1
        buf.put((byte) (bytes.length + 1));

        // content
        buf.put(bytes);
    }


    static int readVarInt(ByteBuffer buf) {
        int value = 0;
        int shift = 0;

        while (true) {
            byte b = buf.get();
            value |= (b & 0x7F) << shift;

            if ((b & 0x80) == 0) break;
            shift += 7;
        }

        return value;
    }

    static void parseValue(byte[] value) {
        ByteBuffer b = ByteBuffer.wrap(value);
        b.order(ByteOrder.BIG_ENDIAN);

        short version = b.getShort();   // skip version
        byte recordType = b.get();      // 👈 KEY
        System.out.println("Version: " + version);
        System.out.println("RecordType: " + recordType);

        if (recordType == 2) {
            //parseTopicRecord(b);
        } else if (recordType == 3) {
            //parsePartitionRecord(b);
        }
    }
}
