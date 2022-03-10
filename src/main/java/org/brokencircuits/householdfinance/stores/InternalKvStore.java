package org.brokencircuits.householdfinance.stores;

import java.io.Closeable;

public interface InternalKvStore<K, V> extends KvStore<K, V>, Closeable {

  void initialize();

  void putRaw(byte[] key, byte[] value);

  byte[] getRaw(byte[] key);

  byte[] serializeKey(K key);

  byte[] serializeValue(V value);

  void clear();
}
