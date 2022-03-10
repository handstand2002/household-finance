package org.brokencircuits.householdfinance.kafka.state;

import java.util.Arrays;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.brokencircuits.householdfinance.util.Pair;
import org.brokencircuits.householdfinance.stores.CloseableIterator;
import org.brokencircuits.householdfinance.stores.InternalKvStore;
import org.brokencircuits.householdfinance.stores.KvStore;

@Slf4j
@RequiredArgsConstructor
public class BackedStore<K, V> implements KvStore<K, V> {

  private final InternalKvStore<K, V> inner;
  private final String underlyingTopicName;
  private final Producer<byte[], byte[]> producer;

  public Optional<V> get(K key) {
    return this.inner.get(key);
  }

  public CloseableIterator<Pair<K, V>> range(K start, K end) {
    return this.inner.range(start, end);
  }

  public CloseableIterator<Pair<K, V>> all() {
    return this.inner.all();
  }

  public void put(K key, V value) {
    byte[] serializedKey = inner.serializeKey(key);
    byte[] serializedValue = inner.serializeValue(value);

    byte[] existingValue = this.inner.getRaw(serializedKey);
    if (Arrays.equals(existingValue, serializedValue)) {
      log.debug("Value identical to existing value, taking no action for put({}, {})", key, value);
      return;
    }

    this.inner.putRaw(serializedKey, serializedValue);

    log.debug("Producing to {}: {} | {}", underlyingTopicName, key, value);
    this.producer.send(new ProducerRecord<>(underlyingTopicName, serializedKey, serializedValue),
        (metadata, exception) -> {
          if (exception == null) {
            log.debug("Produced to {}: {} | {}", metadata, key, value);
          } else {
            log.warn("Producing to {}: {} | {} had exception", underlyingTopicName, key, value,
                exception);
          }
        });
  }

}
