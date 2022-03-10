package org.brokencircuits.householdfinance.stores;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.kafka.common.serialization.Serde;

@Value
@Builder
public class StoreConfig<K, V> {

  @NonNull
  String name;
  /**
   * KeySerde must not require topic to be passed to serializer or deserializer. Since this is a
   * local store, there is no topic and so the topic argument will be null.
   */
  Serde<K> keySerde;
  /**
   * ValueSerde must not require topic to be passed to serializer or deserializer. Since this is a
   * local store, there is no topic and so the topic argument will be null.
   */
  Serde<V> valueSerde;
}
