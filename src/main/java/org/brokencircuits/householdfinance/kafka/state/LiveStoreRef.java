package org.brokencircuits.householdfinance.kafka.state;

import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
@EqualsAndHashCode(callSuper = true)
public class LiveStoreRef<K, V> extends StoreRef<K, V> {

  public LiveStoreRef(String topicName, Serde<K> keySerde, Serde<V> valueSerde) {
    super(keySerde, valueSerde);
    this.topicName = topicName;
  }

  String topicName;
}
