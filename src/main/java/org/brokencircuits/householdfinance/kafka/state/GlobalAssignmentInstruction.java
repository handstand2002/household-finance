package org.brokencircuits.householdfinance.kafka.state;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.brokencircuits.householdfinance.kafka.consume.ConsumerThread.AssignmentInstruction;
import org.jetbrains.annotations.Nullable;

/**
 * Assign the consumer to all partitions for specified topics and seek to beginning
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class GlobalAssignmentInstruction implements AssignmentInstruction {
  private final Collection<String> topics;
  @Nullable
  private final java.util.function.Consumer<Consumer<?,?>> postAssignmentAction;

  @Override
  public void apply(Consumer<?, ?> consumer) {
    Set<String> topicSet = new HashSet<>(topics);

    List<TopicPartition> partitions = consumer.listTopics().entrySet().stream()
        .filter(e -> topicSet.contains(e.getKey()))
        .flatMap(e -> e.getValue().stream()
            .map(p -> new TopicPartition(p.topic(), p.partition())))
        .collect(Collectors.toList());
    consumer.assign(partitions);
    consumer.seekToBeginning(partitions);

    if (postAssignmentAction != null) {
      postAssignmentAction.accept(consumer);
    }
  }
}
