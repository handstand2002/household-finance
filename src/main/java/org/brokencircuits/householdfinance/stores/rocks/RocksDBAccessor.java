package org.brokencircuits.householdfinance.stores.rocks;

import java.util.Collection;
import java.util.List;
import org.apache.kafka.common.utils.Bytes;
import org.brokencircuits.householdfinance.util.Pair;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

public interface RocksDBAccessor {
  void put(byte[] var1, byte[] var2);

  void prepareBatch(List<Pair<Bytes, byte[]>> var1, WriteBatch var2) throws RocksDBException;

  byte[] get(byte[] var1) throws RocksDBException;

  byte[] getOnly(byte[] var1) throws RocksDBException;

  KeyValueIterator<Bytes, byte[]> range(Bytes var1, Bytes var2, boolean var3);

  KeyValueIterator<Bytes, byte[]> all(boolean var1);

  KeyValueIterator<Bytes, byte[]> prefixScan(Bytes var1);

  long approximateNumEntries() throws RocksDBException;

  void flush() throws RocksDBException;

  void prepareBatchForRestore(Collection<Pair<byte[], byte[]>> var1, WriteBatch var2) throws RocksDBException;

  void addToBatch(byte[] var1, byte[] var2, WriteBatch var3) throws RocksDBException;

  void close();
}