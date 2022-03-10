package org.brokencircuits.householdfinance.kafka.produce;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serializer;

public interface ProducerFactory {
  <K, V> Producer<K, V> create(
      Properties props, Serializer<K> keySerializer, Serializer<V> valueSerializer);

  static ProducerFactory defaultFactory() {
    return KafkaProducer::new;
  }
}
