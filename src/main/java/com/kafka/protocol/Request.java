package com.kafka.protocol;

public record Request(Header header, RequestBody body) {
}
