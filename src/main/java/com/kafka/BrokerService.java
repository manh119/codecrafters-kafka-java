package com.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Mirrors Go's Broker struct and all its methods in broker.go.
 *
 * Responsibilities:
 *  - Listens on BROKER_PORT for incoming connections.
 *  - Handles ECHO and P_REG messages synchronously.
 *  - For each registered producer, spawns a goroutine-equivalent (virtual thread)
 *    that dials back to the producer's port and processes PCM messages.
 */
@Slf4j
@Service
public class BrokerService {

    //@Value("${broker.port:10000}")
    private int brokerPort = 10000;

    /** Mirrors Go's []com.kafka.Topic slice — protected by synchronized on this. */
    private final List<Topic> topics = new ArrayList<>();

    /** Spring-managed prototype factory so each com.kafka.Topic gets its own queue. */
    private final ObjectProvider<MessageQueue> queueFactory;

    public BrokerService(ObjectProvider<MessageQueue> queueFactory) {
        this.queueFactory = queueFactory;
    }

    // ------------------------------------------------------------------
    // startBrokerServer — mirrors Go's startBrokerServer()
    // ------------------------------------------------------------------

    /**
     * Blocking server loop — mirrors Go's startBrokerServer().
     * Each accepted connection is handled synchronously (one com.kafka.message per connection),
     * matching the Go behaviour.
     */
    public void startBrokerServer() throws IOException {
        try (ServerSocket server = new ServerSocket(brokerPort)) {
            log.info("Broker listening on port {}", brokerPort);
            //noinspection InfiniteLoopStatement
            while (true) {
                Socket conn = server.accept(); // blocks
                handleConnection(conn);
            }
        }
    }

    private void handleConnection(Socket conn) {
        try (conn;
             var in  = new DataInputStream(conn.getInputStream());
             var out = new DataOutputStream(conn.getOutputStream())) {

            Message msg = Message.readFrom(in);
            Message response = processBrokerMessage(msg);
            if (response != null) {
                response.writeTo(out);
            }
        } catch (IOException e) {
            log.error("Error handling connection: {}", e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // processBrokerMessage — mirrors Go's processBrokerMessage()
    // ------------------------------------------------------------------

    private Message processBrokerMessage(Message message) throws IOException {
        if (message.getEcho() != null) {
            String reply = processEchoMessage(message.getEcho());
            return Message.builder().rEcho(reply).build();
        }
        if (message.getPReg() != null) {
            Byte reply = processProducerRegisterMessage(message.getPReg());
            return Message.builder().rPReg(reply).build();
        }

        if (message.getCReg() != null) {
            Byte reply = processProducerRegisterMessage(message.getPReg());
            return Message.builder().rPReg(reply).build();
        }
        return null;
    }

    // ------------------------------------------------------------------
    // processEchoMessage — mirrors Go's processEchoMessage()
    // ------------------------------------------------------------------

    private String processEchoMessage(String text) {
        return "I have receiver: " + text;
    }

    // ------------------------------------------------------------------
    // processProducerRegisterMessage — mirrors Go's processProducerRegisterMessage()
    // ------------------------------------------------------------------

    private synchronized Byte processProducerRegisterMessage(ProducerRegisterMessage pReg) {
        log.info("Broker received pRegMessage: port={}, topicId={}", pReg.getPort(), pReg.getTopicId());

        // Find or create topic
        int topicIdx = -1;
        for (int i = 0; i < topics.size(); i++) {
            if (topics.get(i).getTopicId() == pReg.getTopicId()) {
                topicIdx = i;
                break;
            }
        }
        if (topicIdx == -1) {
            MessageQueue mq = queueFactory.getObject();
            topics.add(new Topic(pReg.getTopicId(), mq));
            topicIdx = topics.size() - 1;
        }

        final int finalTopicIdx = topicIdx;

        // Dial back to producer on its port — mirrors Go's goroutine
        Thread.ofVirtual().start(() -> connectToProducer(pReg.getPort(), finalTopicIdx));

        return (byte) 0;
    }

    // ------------------------------------------------------------------
    // connectToProducer — mirrors the goroutine inside processProducerRegisterMessage
    // ------------------------------------------------------------------

    /**
     * Dials the producer's port and processes an unlimited stream of PCM messages.
     * Runs on a virtual thread (Java 21 feature, available in preview on Java 17 via
     * Thread.ofVirtual(); replace with new Thread(...) if on Java 17 without preview).
     */
    private void connectToProducer(int port, int topicIdx) {
        try (Socket conn = new Socket("localhost", port);
             var in  = new DataInputStream(conn.getInputStream());
             var out = new DataOutputStream(conn.getOutputStream())) {

            log.info("Connected to producer server at port {}", port);

            //noinspection InfiniteLoopStatement
            while (true) {
                Message msg = Message.readFrom(in);
                if (msg.getPcm() != null) {
                    byte result = processProducerPCM(msg.getPcm(), topicIdx);
                    Message.builder().rPcm(result).build().writeTo(out);
                }
            }
        } catch (IOException e) {
            log.error("Producer connection on port {} ended: {}", port, e.getMessage());
        }
    }

    private void connectToConsumer(int port, int topicIdx) {
        try (Socket conn = new Socket("localhost", port);
             var in  = new DataInputStream(conn.getInputStream());
             var out = new DataOutputStream(conn.getOutputStream())) {

            log.info("Connected to consumer server at port {}", port);

            // event loop
            while (true) {
                Message msg = Message.readFrom(in);
                if (msg.getRPcm() != null) {
                    byte result = processProducerPCM(msg.getPcm(), topicIdx);
                    Message.builder().rPcm(result).build().writeTo(out);
                }
            }
        } catch (IOException e) {
            log.error("Producer connection on port {} ended: {}", port, e.getMessage());
        }
    }

    // ------------------------------------------------------------------
    // processProducerPCM — mirrors Go's processProducerPCM()
    // ------------------------------------------------------------------

    private synchronized byte processProducerPCM(byte[] pcm, int topicIdx) {
        topics.get(topicIdx).getMq().push(pcm);
        topics.get(topicIdx).getMq().debug();
        return (byte) 0;
    }

    private synchronized Byte processConsumerRegisterMessage(ConsumerRegisterMessage consumerRegisterMessage) {
        log.info("Broker received consumer message: port={}, topicId={}", consumerRegisterMessage.getPort(), consumerRegisterMessage.getTopicId());

        // Find or create topic
        int topicIdx = -1;
        for (int i = 0; i < topics.size(); i++) {
            if (topics.get(i).getTopicId() == pReg.getTopicId()) {
                topicIdx = i;
                break;
            }
        }
        if (topicIdx == -1) {
            MessageQueue mq = queueFactory.getObject();
            topics.add(new Topic(pReg.getTopicId(), mq));
            topicIdx = topics.size() - 1;
        }

        final int finalTopicIdx = topicIdx;

        // Dial back to producer on its port — mirrors Go's goroutine
        Thread.ofVirtual().start(() -> connectToProducer(pReg.getPort(), finalTopicIdx));

        return (byte) 0;
    }
}
