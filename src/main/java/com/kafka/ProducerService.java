package com.kafka;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
public class ProducerService {

    private static final int BROKER_PORT = 10000;

    private final int port;
    private final int topicId;

    public ProducerService(int port, int topicId) {
        this.port    = port;
        this.topicId = topicId;
    }

    // ------------------------------------------------------------------
    // startProducerServer — mirrors Go's startProducerServer()
    // ------------------------------------------------------------------

    public void startProducerServer() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            log.info("Producer listening on port {}", port);

            sendPortDataToBroker();

            // Accept exactly one connection from the broker (mirrors ln.Accept in Go)
            try (Socket conn = serverSocket.accept();
                 var in   = new DataInputStream(conn.getInputStream());
                 var out  = new DataOutputStream(conn.getOutputStream());
                 var stdin = new BufferedReader(new InputStreamReader(System.in))) {

                log.info("Broker connected to producer on port {}", port);

                String line;
                while ((line = stdin.readLine()) != null) {
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

            ProducerRegisterMessage pReg = new ProducerRegisterMessage(port, topicId);
            log.info("Sending P_REG: port={}, topicId={}", pReg.getPort(), pReg.getTopicId());

            Message.builder().pReg(pReg).build().writeTo(out);

            Message resp = Message.readFrom(in);
            log.info("Received R_P_REG from broker: {}", resp.getRPReg());
        }
    }
}
