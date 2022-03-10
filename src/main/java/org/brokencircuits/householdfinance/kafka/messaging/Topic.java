package org.brokencircuits.householdfinance.kafka.messaging;

import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
public class Topic<K,V> {

  String name;
  Serde<K> keySerde;
  Serde<V> valueSerde;
}
