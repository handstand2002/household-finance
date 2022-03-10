package org.brokencircuits.householdfinance.kafka.serde;

import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class AppendedTopicSerdeWrapper<T> implements Serde<T> {

  private final Serde<T> inner;
  private final Serializer<T> serializer;
  private final Deserializer<T> deserializer;

  public AppendedTopicSerdeWrapper(Serde<T> inner, String appendToTopic) {
    this.inner = inner;
    this.serializer = (topic, data) -> {
      String newTopic = topic + appendToTopic;
      return inner.serializer().serialize(newTopic, data);
    };
    this.deserializer = (topic, data) -> {
      String newTopic = topic + appendToTopic;
      return inner.deserializer().deserialize(newTopic, data);
    };
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    inner.configure(configs, isKey);
  }

  @Override
  public void close() {
    inner.close();
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
