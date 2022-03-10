package org.brokencircuits.householdfinance.kafka.state;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.brokencircuits.householdfinance.kafka.consume.ConsumerThread.BatchFinalizer;
import org.jetbrains.annotations.Nullable;

/**
 * Update provided {@link #endOffsetsOfUncaughtPartitions} map by removing {@link TopicPartition}
 * keys that are caught up.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class GlobalBatchFinalizer implements BatchFinalizer {

  private final Map<TopicPartition, Long> endOffsetsOfUncaughtPartitions;
  @Nullable
  private final java.util.function.Consumer<Consumer<?, ?>> onCatchUpComplete;
  private boolean isCaughtUp = false;

  @Override
  public void apply(Consumer<?, ?> consumer) {
    if (isCaughtUp) {
      return;
    }
    Collection<TopicPartition> caughtPartitions = new LinkedList<>();
    endOffsetsOfUncaughtPartitions.forEach((tp, endOffset) -> {
      if (consumer.position(tp) >= endOffset) {
        caughtPartitions.add(tp);
      }
    });
    for (TopicPartition caughtPartition : caughtPartitions) {
      endOffsetsOfUncaughtPartitions.remove(caughtPartition);
    }
    if (endOffsetsOfUncaughtPartitions.isEmpty()) {
      isCaughtUp = true;
      if (onCatchUpComplete != null) {
        onCatchUpComplete.accept(consumer);
      }
    }
  }
}
