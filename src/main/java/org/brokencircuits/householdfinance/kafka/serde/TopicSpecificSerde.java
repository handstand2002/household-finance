package org.brokencircuits.householdfinance.kafka.serde;

import lombok.experimental.Delegate;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TopicSpecificSerde<T> implements Serde<T> {

  @Delegate(excludes = Implemented.class)
  private final Serde<T> inner;
  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public TopicSpecificSerde(String topic, Serde<T> inner) {
    this.inner = inner;
    serializer = new TopicSpecificSerializer<>(topic, inner.serializer());
    deserializer = new TopicSpecificDeserializer<>(topic, inner.deserializer());
  }

  private interface Implemented {

    <U> Serializer<U> serializer();

    <U> Deserializer<U> deserializer();
  }

  @Override
  public Serializer<T> serializer() {
    return serializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return deserializer;
  }
}
