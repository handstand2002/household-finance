package org.brokencircuits.householdfinance.stores.rocks;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.brokencircuits.householdfinance.util.Pair;
import org.brokencircuits.householdfinance.stores.CloseableIterator;
import org.brokencircuits.householdfinance.stores.InternalKvStore;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

@Slf4j
@RequiredArgsConstructor
public class RocksDbStore<K, V> implements InternalKvStore<K, V> {

  private final File baseDir;
  private final String dbName;
  private final Serde<K> keySerde;
  private final Serde<V> valueSerde;
  private RocksDB db;
  private boolean open = false;
  private SingleColumnFamilyAccessor dbAccessor;
  private final Set<KeyValueIterator<Bytes, byte[]>> openIterators = Collections
      .synchronizedSet(new HashSet<>());

  public void initialize() {
    RocksDB.loadLibrary();
    File dbDir = new File(baseDir, dbName);

    WriteOptions wOptions = new WriteOptions();
    wOptions.setDisableWAL(true);

    FlushOptions fOptions = new FlushOptions();
    fOptions.setWaitForFlush(true);

    final DBOptions dbOptions = new DBOptions();
    dbOptions.setCreateIfMissing(true);
    final ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
    List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections
        .singletonList(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
    List<ColumnFamilyHandle> columnFamilies = new ArrayList<>(columnFamilyDescriptors.size());

    try {
      Files.createDirectories(dbDir.getParentFile().toPath());
      Files.createDirectories(dbDir.getAbsoluteFile().toPath());
      this.db = RocksDB
          .open(dbOptions, dbDir.getAbsolutePath(), columnFamilyDescriptors, columnFamilies);
      this.dbAccessor = new SingleColumnFamilyAccessor(this.db, columnFamilies.get(0), wOptions,
          fOptions, dbName, openIterators);
      open = true;
    } catch (RocksDBException | IOException e) {
      throw new IllegalStateException(
          String.format("Could not open %s on path %s", getClass().getSimpleName(), dbDir), e);
    }
  }

  @Override
  public void put(K key, V value) {
    putRaw(serializeKey(key), serializeValue(value));
  }

  @Override
  public byte[] getRaw(byte[] key) {
    assertOpen();
    Objects.requireNonNull(key, "Key cannot be null");
    try {
      return dbAccessor.get(key);
    } catch (RocksDBException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void putRaw(byte[] key, byte[] value) {
    assertOpen();
    dbAccessor.put(key, value);
  }

  public byte[] serializeValue(V value) {
    try {
      return valueSerde.serializer().serialize(null, value);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          serializationErrorMsg(SerializationAction.SERIALIZE, KeyOrValue.VALUE), e);
    }
  }

  @Override
  public void clear() {
    try {
      for (String file : db.getLiveFiles().files) {
        db.deleteFile(file);
      }
    } catch (RocksDBException e) {
      throw new IllegalStateException(e);
    }
  }

  @Getter
  @RequiredArgsConstructor
  private enum SerializationAction {
    SERIALIZE("serializing", "Serializer"),
    DESERIALIZE("deserializing", "Deserializer");

    private final String action;
    private final String objectType;
  }

  @Getter
  @RequiredArgsConstructor
  private enum KeyOrValue {
    KEY("key"), VALUE("value");
    private final String friendly;
  }

  private String serializationErrorMsg(
      @NotNull RocksDbStore.SerializationAction serializationAction,
      @NotNull KeyOrValue keyOrValue) {

    return String.format("Exception %s %s for %s %s. %s may rely on topic argument, "
            + "which is not supported. Use deserializer that doesn't rely on topic name",
        serializationAction.getAction(), keyOrValue.getFriendly(), getClass().getSimpleName(),
        dbName, serializationAction.getObjectType());
  }

  public byte[] serializeKey(K key) {
    try {
      return keySerde.serializer().serialize(null, key);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          serializationErrorMsg(SerializationAction.SERIALIZE, KeyOrValue.KEY), e);
    }
  }

  private void assertOpen() {
    if (!open) {
      throw new IllegalStateException(String.format("%s is not open", getClass().getSimpleName()));
    }
  }

  @Override
  public Optional<V> get(K key) {
    return Optional.ofNullable(getRaw(serializeKey(key)))
        .map(this::deserializeValue);
  }

  private V deserializeValue(byte[] valueBytes) {
    try {
      return valueSerde.deserializer().deserialize(null, valueBytes);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          serializationErrorMsg(SerializationAction.DESERIALIZE, KeyOrValue.VALUE), e);
    }
  }

  private K deserializeKey(byte[] keyBytes) {
    try {
      return keySerde.deserializer().deserialize(null, keyBytes);
    } catch (RuntimeException e) {
      throw new RuntimeException(
          serializationErrorMsg(SerializationAction.DESERIALIZE, KeyOrValue.KEY), e);
    }
  }

  @Override
  public CloseableIterator<Pair<K, V>> range(K start, K end) {
    byte[] startBytes = serializeKey(start);
    byte[] endBytes = serializeKey(end);
    KeyValueIterator<Bytes, byte[]> iter = dbAccessor
        .range(Bytes.wrap(startBytes), Bytes.wrap(endBytes), true);
    openIterators.add(iter);
    return getCloseableIterator(iter);
  }

  @Override
  public CloseableIterator<Pair<K, V>> all() {

    KeyValueIterator<Bytes, byte[]> iter = dbAccessor.all(true);
    openIterators.add(iter);
    return getCloseableIterator(iter);
  }

  @NotNull
  private CloseableIterator<Pair<K, V>> getCloseableIterator(KeyValueIterator<Bytes, byte[]> iter) {
    return new CloseableIterator<Pair<K, V>>() {
      @Override
      public void close() throws Exception {
        iter.close();
      }

      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public Pair<K, V> next() {
        Pair<Bytes, byte[]> kv = iter.next();
        K k = deserializeKey(kv.getKey().get());
        V v = deserializeValue(kv.getValue());
        return Pair.of(k, v);
      }
    };
  }

  @Override
  public void close() {
    db.close();
  }
}