import client.Client;
import kafka.Kafka;
import protocol.ExchangeMapper;

import java.net.ServerSocket;

public class Main {

    public static final int PORT = 9092;

    public static void main(String[] args) {
        final var kafka = Kafka.load("/tmp/kraft-combined-logs/");

        final var exchangeMapper = new ExchangeMapper();

        System.out.println("listen: %d".formatted(PORT));
        try (final var serverSocket = new ServerSocket(PORT)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                final var clientSocket = serverSocket.accept();
                System.out.println("connected: %s".formatted(clientSocket.getRemoteSocketAddress()));

                Thread.ofVirtual().start(new Client(kafka, exchangeMapper, clientSocket));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

