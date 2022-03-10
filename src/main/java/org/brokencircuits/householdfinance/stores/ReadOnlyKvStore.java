package org.brokencircuits.householdfinance.stores;

import java.util.Optional;
import org.brokencircuits.householdfinance.util.Pair;

public interface ReadOnlyKvStore<K,V> {

  Optional<V> get(K key);

  CloseableIterator<Pair<K, V>> range(K start, K end);

  CloseableIterator<Pair<K, V>> all();
}
