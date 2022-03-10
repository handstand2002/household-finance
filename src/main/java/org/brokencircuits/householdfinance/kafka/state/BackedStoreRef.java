package org.brokencircuits.householdfinance.kafka.state;

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;
import org.brokencircuits.householdfinance.kafka.messaging.Topic;

@Value
@EqualsAndHashCode(callSuper = true)
public class BackedStoreRef<K, V> extends StoreRef<K, V> {

  String name;
  BackedTopicType type;

  public BackedStoreRef(String name, Serde<K> keySerde, Serde<V> valueSerde) {
    super(keySerde, valueSerde);
    this.name = name;
    this.type = BackedTopicType.INTERNAL;
  }

  public BackedStoreRef(Topic<K,V> underlyingTopic) {
    super(underlyingTopic.getKeySerde(), underlyingTopic.getValueSerde());
    this.name = underlyingTopic.getName();
    this.type = BackedTopicType.PUBLIC;
  }

}
