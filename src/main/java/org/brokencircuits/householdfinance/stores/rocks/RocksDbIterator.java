package org.brokencircuits.householdfinance.stores.rocks;

import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Bytes;
import org.brokencircuits.householdfinance.util.Pair;
import org.rocksdb.RocksIterator;

public class RocksDbIterator extends AbstractIterator<Pair<Bytes, byte[]>> implements
    KeyValueIterator<Bytes, byte[]> {

  private final String storeName;
  private final RocksIterator iter;
  private final Set<KeyValueIterator<Bytes, byte[]>> openIterators;
  private final Consumer<RocksIterator> advanceIterator;

  private volatile boolean open = true;

  private Pair<Bytes, byte[]> next;

  RocksDbIterator(final String storeName,
      final RocksIterator iter,
      final Set<KeyValueIterator<Bytes, byte[]>> openIterators,
      final boolean forward) {
    this.storeName = storeName;
    this.iter = iter;
    this.openIterators = openIterators;
    this.advanceIterator = forward ? RocksIterator::next : RocksIterator::prev;
  }

  @Override
  public synchronized boolean hasNext() {
    if (!open) {
      throw new IllegalStateException(
          String.format("RocksDB iterator for store %s has closed", storeName));
    }
    return super.hasNext();
  }

  @Override
  public Pair<Bytes, byte[]> makeNext() {
    if (!iter.isValid()) {
      return allDone();
    } else {
      next = getKeyValue();
      advanceIterator.accept(iter);
      return next;
    }
  }

  private Pair<Bytes, byte[]> getKeyValue() {
    return new Pair<>(new Bytes(iter.key()), iter.value());
  }

  @Override
  public synchronized void close() {
    openIterators.remove(this);
    iter.close();
    open = false;
  }

  @Override
  public Bytes peekNextKey() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return next.getKey();
  }
}
