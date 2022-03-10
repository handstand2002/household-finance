package org.brokencircuits.householdfinance.kafka.produce;

import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.RecordMetadata;

public interface Publisher<K,V> {

  Future<RecordMetadata> send(K key, V value);
}
