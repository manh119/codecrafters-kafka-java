package com.kafka;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Mirrors Go's Producer struct and its methods in producer.go.
 *
 * Flow:
 *  1. Open a ServerSocket on p.port  (ln = net.Listen)
 *  2. Send P_REG to the broker       (sendPortDataToBroker)
 *  3. Accept broker's dial-back      (conn = ln.Accept)
 *  4. Loop: read stdin → send PCM → receive R_PCM
 */
@Slf4j
public class Consumer {
    private static final int BROKER_PORT = 10000;

    private final int port;
    private final int topicId;

    public Consumer(int port, int topicId) {
        this.port    = port;
        this.topicId = topicId;
    }

    public void startConsumerServer() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("Consumer listening on port {}", port);

            sendPortDataToBroker();

            // Accept exactly one connection from the broker
            try (Socket conn = serverSocket.accept();
                 var in   = new DataInputStream(conn.getInputStream());
                 var out  = new DataOutputStream(conn.getOutputStream());) {

                log.info("Broker connected to consumer on port {}", port);

                String line = null;
                while (in != null) {
                    line = line + "\n"; // preserve Go's ReadString('\n') behaviour

                    // Send PCM
                    Message.builder().pcm(line.getBytes()).build().writeTo(out);

                    // Read R_PCM back
                    Message resp = Message.readFrom(in);
                    log.info("Received R_PCM from broker: {}", resp.getRPcm());
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // sendPortDataToBroker — mirrors Go's sendPortDataToBroker()
    // ------------------------------------------------------------------

    private void sendPortDataToBroker() throws IOException {
        try (Socket conn = new Socket("localhost", BROKER_PORT);
             var in  = new DataInputStream(conn.getInputStream());
             var out = new DataOutputStream(conn.getOutputStream())) {

            ConsumerRegisterMessage cReg = new ConsumerRegisterMessage(port, topicId);
            log.info("Sending C_REG: port={}, topicId={}", cReg.getPort(), cReg.getTopicId());

            Message.builder().cReg(cReg).build().writeTo(out);

            Message resp = Message.readFrom(in);
            //log.info("Received R_C_REG from broker: {}", resp.getRCcm());
        }
    }
}
