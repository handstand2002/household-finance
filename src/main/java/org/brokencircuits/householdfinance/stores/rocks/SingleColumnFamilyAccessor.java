package org.brokencircuits.householdfinance.stores.rocks;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.utils.Bytes;
import org.brokencircuits.householdfinance.util.Pair;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class SingleColumnFamilyAccessor implements RocksDBAccessor {
  private final ColumnFamilyHandle columnFamily;
  private final RocksDB db;
  private final FlushOptions fOptions;
  private final WriteOptions wOptions;
  private final String dbName;
  private final Set<KeyValueIterator<Bytes, byte[]>> openIterators;

  SingleColumnFamilyAccessor(RocksDB db, ColumnFamilyHandle columnFamily,
      WriteOptions wOptions, FlushOptions fOptions, String dbName,
      Set<KeyValueIterator<Bytes, byte[]>> openIterators) {
    this.columnFamily = columnFamily;
    this.db = db;
    this.wOptions = wOptions;
    this.fOptions = fOptions;
    this.dbName = dbName;
    this.openIterators = openIterators;
  }

  public void put(byte[] key, byte[] value) {
    if (value == null) {
      try {
        this.db.delete(this.columnFamily, this.wOptions, key);
      } catch (RocksDBException var5) {
        throw new IllegalStateException("Error while removing key from store " + this.dbName, var5);
      }
    } else {
      try {
        this.db.put(this.columnFamily, this.wOptions, key, value);
      } catch (RocksDBException var4) {
        throw new IllegalStateException("Error while putting key/value into store " + this.dbName, var4);
      }
    }

  }

  public void prepareBatch(List<Pair<Bytes, byte[]>> entries, WriteBatch batch) throws RocksDBException {
    Iterator var3 = entries.iterator();

    while(var3.hasNext()) {
      Pair<Bytes, byte[]> entry = (Pair)var3.next();
      Objects.requireNonNull(entry.getKey(), "key cannot be null");
      this.addToBatch(entry.getKey().get(), entry.getValue(), batch);
    }

  }

  public byte[] get(byte[] key) throws RocksDBException {
    return this.db.get(this.columnFamily, key);
  }

  public byte[] getOnly(byte[] key) throws RocksDBException {
    return this.db.get(this.columnFamily, key);
  }

  public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to, boolean forward) {
    return new RocksDBRangeIterator(this.dbName, this.db.newIterator(this.columnFamily), this.openIterators, from, to, forward, true);
  }

  public KeyValueIterator<Bytes, byte[]> all(boolean forward) {
    RocksIterator innerIterWithTimestamp = this.db.newIterator(this.columnFamily);
    if (forward) {
      innerIterWithTimestamp.seekToFirst();
    } else {
      innerIterWithTimestamp.seekToLast();
    }

    return new RocksDbIterator(this.dbName, innerIterWithTimestamp, this.openIterators, forward);
  }

  public KeyValueIterator<Bytes, byte[]> prefixScan(Bytes prefix) {
    Bytes to = Bytes.increment(prefix);
    return new RocksDBRangeIterator(this.dbName, this.db.newIterator(this.columnFamily), this.openIterators, prefix, to, true, false);
  }

  public long approximateNumEntries() throws RocksDBException {
    return this.db.getLongProperty(this.columnFamily, "rocksdb.estimate-num-keys");
  }

  public void flush() throws RocksDBException {
    this.db.flush(this.fOptions, this.columnFamily);
  }

  public void prepareBatchForRestore(Collection<Pair<byte[], byte[]>> records, WriteBatch batch) throws RocksDBException {
    Iterator var3 = records.iterator();

    while(var3.hasNext()) {
      Pair<byte[], byte[]> record = (Pair)var3.next();
      this.addToBatch(record.getKey(), record.getValue(), batch);
    }
  }

  public void addToBatch(byte[] key, byte[] value, WriteBatch batch) throws RocksDBException {
    if (value == null) {
      batch.delete(this.columnFamily, key);
    } else {
      batch.put(this.columnFamily, key, value);
    }

  }

  public void close() {
    this.columnFamily.close();
  }
}
