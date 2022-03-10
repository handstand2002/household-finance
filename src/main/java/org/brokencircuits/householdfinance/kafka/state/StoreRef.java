package org.brokencircuits.householdfinance.kafka.state;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.common.serialization.Serde;

@AllArgsConstructor(access = AccessLevel.PACKAGE)
@Getter
class StoreRef<K, V> {

  protected Serde<K> keySerde;
  protected Serde<V> valueSerde;
}
