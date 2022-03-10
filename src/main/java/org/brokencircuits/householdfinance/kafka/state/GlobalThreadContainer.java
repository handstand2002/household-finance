package org.brokencircuits.householdfinance.kafka.state;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.brokencircuits.householdfinance.kafka.consume.ConsumerFactory;
import org.brokencircuits.householdfinance.kafka.consume.ConsumerThread;
import org.brokencircuits.householdfinance.kafka.messaging.KafkaConfig;
import org.brokencircuits.householdfinance.stores.InternalKvStore;

@Slf4j
@RequiredArgsConstructor
public class GlobalThreadContainer {

  private final KafkaConfig config;
  private final Map<LiveStoreRef<?, ?>, InternalKvStore<?, ?>> liveStoreRefs;
  private final ConsumerFactory consumerFactory;
  private ConsumerThread thread;

  public CompletableFuture<Void> initialize() {
    Map<String, AtomicLong> recordsReceivedByTopic = new HashMap<>();
    AtomicBoolean doingRestore = new AtomicBoolean(false);
    log.info("Initializing global thread for stores: {}", liveStoreRefs.keySet());
    CompletableFuture<Void> initCompleteFuture = new CompletableFuture<>();
    Map<String, StoreDetails<?, ?>> storeDetailsByTopicName = new HashMap<>();
    liveStoreRefs.forEach(
        (ref, store) -> storeDetailsByTopicName.compute(ref.getTopicName(), (t, details) -> {
          if (details != null) {
            throw new IllegalStateException("More than 1 LiveStoreRef for topic " + t);
          }
          return new StoreDetails(ref, store);
        }));

    List<String> allTopics = liveStoreRefs.keySet().stream().map(LiveStoreRef::getTopicName)
        .collect(Collectors.toList());
    Properties consumerProps = config.getConsumer();
    consumerProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    AtomicReference<Instant> assignmentTime = new AtomicReference<>(null);

    Map<TopicPartition, Long> endOffsetsOfUncaughtPartitions = new HashMap<>();
    thread = ConsumerThread.builder()
        .assignmentInstruction(new GlobalAssignmentInstruction(allTopics, c -> {
          endOffsetsOfUncaughtPartitions.putAll(c.endOffsets(c.assignment()));
          assignmentTime.set(Instant.now());
          doingRestore.set(true);
        }))
        .consumerProps(consumerProps)
        .factory(consumerFactory)
        .handleRecords(r -> {
          StoreDetails<?, ?> storeDetails = storeDetailsByTopicName.get(r.topic());
          if (storeDetails == null) {
            log.error("Received record for unregistered topic: {}", r);
            return;
          }
          putRecordInStore(r, storeDetails);
          if (doingRestore.get()) {
            recordsReceivedByTopic.computeIfAbsent(r.topic(), t -> new AtomicLong(0))
                .incrementAndGet();
          }
        })
        .batchFinalizer(new GlobalBatchFinalizer(endOffsetsOfUncaughtPartitions, c -> {
          initCompleteFuture.complete(null);
          String receivedRecordsReport = recordsReceivedByTopic.entrySet().stream()
              .map(e -> String.format("%s: %d", e.getKey(), e.getValue().get()))
              .collect(Collectors.joining("\n\t"));
          recordsReceivedByTopic.clear();
          log.info("State restore complete in {} with records received:\n\t{}",
              Duration.between(assignmentTime.get(), Instant.now()), receivedRecordsReport);
        }))
        .pollDuration(Duration.ofSeconds(5))
        .build();

    thread.setName("global-consumer");
    thread.setDaemon(true);
    thread.start();

    return initCompleteFuture;
  }

  private <K, V> void putRecordInStore(ConsumerRecord<byte[], byte[]> record,
      StoreDetails<K, V> details) {
    InternalKvStore<K, V> store = details.getStore();
    store.putRaw(record.key(), record.value());
  }

  @Value
  private static class StoreDetails<K, V> {

    LiveStoreRef<K, V> ref;
    InternalKvStore<K, V> store;
  }
}
