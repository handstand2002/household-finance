package org.brokencircuits.householdfinance.kafka.produce;

import java.util.concurrent.Future;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.brokencircuits.householdfinance.kafka.messaging.Topic;

@Slf4j
@RequiredArgsConstructor
public class TopicPublisher<K, V> implements Publisher<K, V> {

  private final Producer<K, V> producer;
  private final Topic<K, V> topic;

  @Override
  public Future<RecordMetadata> send(K key, V value) {
    log.info("Publishing to {}: {} | {}", topic.getName(), key, value);
    return producer.send(new ProducerRecord<>(topic.getName(), key, value), (recordMetadata, e) -> {
      if (e == null) {
        log.trace("Published to {}: {} | {}", recordMetadata, key, value);
      } else {
        log.error("Could not publish to {}: {} | {}", topic.getName(), key, value, e);
      }
    });
  }
}
