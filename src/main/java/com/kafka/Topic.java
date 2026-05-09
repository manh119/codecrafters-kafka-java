package com.kafka;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Mirrors Go's com.kafka.Topic struct in topic.go.
 * Each com.kafka.Topic owns a com.kafka.MessageQueue (mq) identified by topicId.
 */
@Slf4j
@Getter
public class Topic {

    private final int          topicId;
    private final MessageQueue mq;

    public Topic(int topicId, MessageQueue mq) {
        this.topicId = topicId;
        this.mq      = mq;
        log.debug("com.kafka.Topic {} initialised", topicId);
    }
}
