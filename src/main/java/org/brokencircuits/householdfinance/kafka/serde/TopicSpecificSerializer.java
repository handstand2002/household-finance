package org.brokencircuits.householdfinance.kafka.serde;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

@RequiredArgsConstructor
public class TopicSpecificSerializer<T> implements Serializer<T> {

  private final String topic;
  @Delegate(excludes = Implemented.class)
  private final Serializer<T> inner;

  private interface Implemented {
    <T> byte[] serialize(String topic, T data);
    <T> byte[] serialize(String topic, Headers headers, T data);
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return inner.serialize(this.topic, data);
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T data) {
    return inner.serialize(this.topic, headers, data);
  }
}
