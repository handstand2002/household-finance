package org.brokencircuits.householdfinance.stores.rocks;


import java.util.Comparator;
import java.util.Set;
import org.apache.kafka.common.utils.Bytes;
import org.brokencircuits.householdfinance.util.Pair;
import org.rocksdb.RocksIterator;

public class RocksDBRangeIterator extends RocksDbIterator {
  // RocksDB's JNI interface does not expose getters/setters that allow the
  // comparator to be pluggable, and the default is lexicographic, so it's
  // safe to just force lexicographic comparator here for now.
  private final Comparator<byte[]> comparator = Bytes.BYTES_LEXICO_COMPARATOR;
  private final byte[] rawLastKey;
  private final boolean forward;
  private final boolean toInclusive;

  RocksDBRangeIterator(final String storeName,
      final RocksIterator iter,
      final Set<KeyValueIterator<Bytes, byte[]>> openIterators,
      final Bytes from,
      final Bytes to,
      final boolean forward,
      final boolean toInclusive) {
    super(storeName, iter, openIterators, forward);
    this.forward = forward;
    this.toInclusive = toInclusive;
    if (forward) {
      iter.seek(from.get());
      rawLastKey = to.get();
      if (rawLastKey == null) {
        throw new NullPointerException("RocksDBRangeIterator: RawLastKey is null for key " + to);
      }
    } else {
      iter.seekForPrev(to.get());
      rawLastKey = from.get();
      if (rawLastKey == null) {
        throw new NullPointerException("RocksDBRangeIterator: RawLastKey is null for key " + from);
      }
    }
  }

  @Override
  public Pair<Bytes, byte[]> makeNext() {
    final Pair<Bytes, byte[]> next = super.makeNext();
    if (next == null) {
      return allDone();
    } else {
      if (forward) {
        if (comparator.compare(next.getKey().get(), rawLastKey) < 0) {
          return next;
        } else if (comparator.compare(next.getKey().get(), rawLastKey) == 0) {
          return toInclusive ? next : allDone();
        } else {
          return allDone();
        }
      } else {
        if (comparator.compare(next.getKey().get(), rawLastKey) >= 0) {
          return next;
        } else {
          return allDone();
        }
      }
    }
  }
}