import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.err.println("Logs from your program will appear here!");

        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        int port = 9092;
        try {
            serverSocket = new ServerSocket(port);
            System.out.println("Server started on port " + port);
            // Since the tester restarts your program quite often, setting SO_REUSEADDR
            // ensures that we don't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            // Wait for connection from client.

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                while (true) {
                    clientSocket = serverSocket.accept();
                    if (clientSocket != null) {
                        InputStream inputStream = clientSocket.getInputStream();

// đọc size
                        byte[] sizeBytes = inputStream.readNBytes(4);
                        int size = ByteBuffer.wrap(sizeBytes).getInt();

                        System.err.println("size=" + size);

// đọc payload
                        byte[] payload = inputStream.readNBytes(size);
                        System.err.println("payload read");

// parse correlation_id
                        ByteBuffer req = ByteBuffer.wrap(payload);
                        req.order(ByteOrder.BIG_ENDIAN);

                        short apiKey = req.getShort();
                        short apiVersion = req.getShort();
                        int correlationId = req.getInt();

                        System.err.println("correlationId=" + correlationId);

// response
                        OutputStream outputStream = clientSocket.getOutputStream();

                        ByteBuffer resp = ByteBuffer.allocate(8);
                        resp.order(ByteOrder.BIG_ENDIAN);
                        resp.putInt(4);
                        resp.putInt(correlationId);

                        System.err.println("Sending response...");

                        outputStream.write(resp.array());
                        outputStream.flush();

                    }
                    //clientSocket.close();
                }

            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
