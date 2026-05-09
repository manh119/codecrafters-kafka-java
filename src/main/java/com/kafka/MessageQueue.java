package com.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Arrays;

/**
 * Fixed-size circular byte-array queue — mirrors Go's Queue struct in queue.go.
 *
 * Go used two global arrays (underArr, underSize); here they are instance fields
 * so each com.kafka.Topic gets its own isolated queue (thread-safe via synchronized).
 *
 * Slot layout:
 *   Each slot is exactly MAX_MESSAGE_SIZE bytes wide.
 *   A parallel size array records the actual payload length of each slot.
 *   head/tail are byte-offsets (multiples of MAX_MESSAGE_SIZE) wrapping at
 *   MAX_MESSAGE_SIZE * CAPACITY.
 */
@Slf4j
@Component
//@Scope("prototype") // one instance per com.kafka.Topic
public class MessageQueue {

    private int maxMessageSize = 255;
    private int capacity = 10000;
    private final int totalSize;

    private final byte[] data;
    private final int[]  sizes; // actual length per slot

    private int head;
    private int tail;

    public MessageQueue() {
//        this.maxMessageSize = maxMessageSize;
//        this.capacity       = capacity;
        this.totalSize      = maxMessageSize * capacity;
        this.data           = new byte[totalSize];
        this.sizes          = new int[capacity];  // indexed by slot number
        this.head           = 0;
        this.tail           = 0;
    }

    /**
     * Pushes a com.kafka.message into the queue (mirrors Go's push).
     * data.length must be <= maxMessageSize.
     */
    public synchronized void push(byte[] payload) {
        if (payload.length > maxMessageSize) {
            throw new IllegalArgumentException(
                "Payload length " + payload.length + " exceeds maxMessageSize " + maxMessageSize);
        }
        System.arraycopy(payload, 0, data, tail, payload.length);
        int slotIdx = tail / maxMessageSize;
        sizes[slotIdx] = payload.length;
        tail = (tail + maxMessageSize) % totalSize;
    }

    /**
     * Pops a com.kafka.message from the queue (mirrors Go's pop).
     */
    public synchronized byte[] pop() {
        int slotIdx = head / maxMessageSize;
        int len     = sizes[slotIdx];
        byte[] result = Arrays.copyOfRange(data, head, head + len);
        head = (head + maxMessageSize) % totalSize;
        return result;
    }

    public synchronized boolean isEmpty() {
        return head == tail;
    }

    /**
     * Prints all current queue entries to the log (mirrors Go's debug()).
     */
    public synchronized void debug() {
        log.debug("Debug queue:");
        int cur = head;
        while (cur != tail) {
            int slotIdx = cur / maxMessageSize;
            int len     = sizes[slotIdx];
            byte[] entry = Arrays.copyOfRange(data, cur, cur + len);
            log.debug("  [{}]", new String(entry).trim());
            cur = (cur + maxMessageSize) % totalSize;
        }
    }
}
