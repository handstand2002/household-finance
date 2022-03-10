package org.brokencircuits.householdfinance.kafka.state;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.brokencircuits.householdfinance.stores.ReadOnlyKvStore;

@RequiredArgsConstructor
public class LiveStore<K, V> implements ReadOnlyKvStore<K, V> {

  @Delegate
  private final ReadOnlyKvStore<K, V> inner;
}
