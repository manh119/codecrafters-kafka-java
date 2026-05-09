package com.kafka;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Socket;

/**
 * Mirrors Go's clientConnectTCPAndEcho() function in main.go.
 *
 * Reads one line from stdin, sends it as an ECHO com.kafka.message,
 * and prints the broker's R_ECHO reply.
 */
@Slf4j
public class EchoClient {

    private final int port;

    public EchoClient(int port) {
        this.port = port;
    }

    public void run() throws IOException {
        try (Socket conn = new Socket("localhost", port);
             var in   = new DataInputStream(conn.getInputStream());
             var out  = new DataOutputStream(conn.getOutputStream());
             var stdin = new BufferedReader(new InputStreamReader(System.in))) {

            log.info("Connected to server at port {}", port);

            String line = stdin.readLine();
            if (line == null) return;

            Message.builder().echo(line + "\n").build().writeTo(out);

            Message resp = Message.readFrom(in);
            System.out.printf("Receive com.kafka.message from server: %s%n", resp.getREcho());
        }
    }
}
