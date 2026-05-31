package com.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 *   java -jar com.kafka.kafka-java.jar server
 *   java -jar com.kafka.kafka-java.jar producer <port> <topicId>
 *   java -jar com.kafka.kafka-java.jar          (echo com.kafka.client, connects to port 10000)
 */
@Slf4j
@SpringBootApplication
public class KafkaApplication implements ApplicationRunner {

    private final BrokerService brokerService;

    public KafkaApplication(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        var nonOptionArgs = args.getNonOptionArgs();

        if (nonOptionArgs.isEmpty()) {
            // Default: echo com.kafka.client on port 10000
            new EchoClient(10000).run();
            return;
        }

        switch (nonOptionArgs.get(0)) {

            case "server" -> {
                log.info("Starting broker server...");
                brokerService.startBrokerServer(); // blocking
            }

            case "producer" -> {
                if (nonOptionArgs.size() < 3) {
                    System.err.println("Usage: producer <port> <topicId>");
                    System.exit(1);
                }
                int port    = Integer.parseInt(nonOptionArgs.get(1));
                int topicId = Integer.parseInt(nonOptionArgs.get(2));
                log.info("Starting producer: port={}, topicId={}", port, topicId);
                new ProducerService(port, topicId).startProducerServer(); // blocking
            }

            case "consumer" -> {
                if (nonOptionArgs.size() < 3) {
                    log.error("Usage: consumer <port> <topicId>");
                    System.exit(1);
                }
                int port    = Integer.parseInt(nonOptionArgs.get(1));
                int topicId = Integer.parseInt(nonOptionArgs.get(2));
                log.info("Starting consumer: port={}, consumerGroupId={}", port, topicId);
                new Consumer(port, topicId).startConsumerServer(); // blocking
            }

            default -> {
                log.info("Unknown mode '{}', starting echo com.kafka.client on port 10000", nonOptionArgs.get(0));
                new EchoClient(10000).run();
            }
        }
    }
}
