package org.brokencircuits.householdfinance.kafka.serde;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

@RequiredArgsConstructor
public class TopicSpecificDeserializer<T> implements Deserializer<T> {

  private final String topic;
  @Delegate(excludes = Implemented.class)
  private final Deserializer<T> inner;

  private interface Implemented {

    <T> T deserialize(String topic, byte[] data);

    <T> T deserialize(String topic, Headers headers, byte[] data);
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    return inner.deserialize(this.topic, data);
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {
    return inner.deserialize(this.topic, headers, data);
  }
}
