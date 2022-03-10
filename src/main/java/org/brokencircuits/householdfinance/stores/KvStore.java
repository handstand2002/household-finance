package org.brokencircuits.householdfinance.stores;

public interface KvStore<K, V> extends ReadOnlyKvStore<K, V> {

  void put(K key, V value);


}