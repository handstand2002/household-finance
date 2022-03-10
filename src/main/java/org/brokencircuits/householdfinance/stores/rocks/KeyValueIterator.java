package org.brokencircuits.householdfinance.stores.rocks;

import java.io.Closeable;
import java.util.Iterator;
import org.brokencircuits.householdfinance.util.Pair;

public interface KeyValueIterator<K, V> extends Iterator<Pair<K, V>>, Closeable {

  @Override
  void close();

  /**
   * Peek at the next key without advancing the iterator
   * @return the key of the next value that would be returned from the next call to next
   */
  K peekNextKey();
}