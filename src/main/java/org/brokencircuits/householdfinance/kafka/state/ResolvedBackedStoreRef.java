package org.brokencircuits.householdfinance.kafka.state;

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
@EqualsAndHashCode(callSuper = true)
public class ResolvedBackedStoreRef<K, V> extends StoreRef<K, V> {

  String topicName;
  BackedTopicType type;

  ResolvedBackedStoreRef(String topicName, Serde<K> keySerde, Serde<V> valueSerde,
      BackedTopicType type) {
    super(keySerde, valueSerde);
    this.topicName = topicName;
    this.type = type;
  }
}
