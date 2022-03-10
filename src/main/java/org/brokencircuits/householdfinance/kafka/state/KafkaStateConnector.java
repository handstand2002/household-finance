package org.brokencircuits.householdfinance.kafka.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.brokencircuits.householdfinance.kafka.admin.AdminClientFactory;
import org.brokencircuits.householdfinance.kafka.consume.ConsumerFactory;
import org.brokencircuits.householdfinance.kafka.messaging.KafkaConfig;
import org.brokencircuits.householdfinance.kafka.produce.ProducerFactory;
import org.brokencircuits.householdfinance.kafka.serde.TopicSpecificSerde;
import org.brokencircuits.householdfinance.stores.InternalKvStore;
import org.brokencircuits.householdfinance.stores.KvStore;
import org.brokencircuits.householdfinance.stores.ReadOnlyKvStore;
import org.brokencircuits.householdfinance.stores.StoreConfig;
import org.brokencircuits.householdfinance.stores.StoreFactory;
import org.jetbrains.annotations.Nullable;

@Slf4j
public class KafkaStateConnector {

  private final StoreFactory storeFactory;
  private final int numPartitions;
  private final short replicationFactor;
  private final AdminClientFactory adminClientFactory;
  private final ConsumerFactory consumerFactory;
  private final ProducerFactory producerFactory;
  @NonNull
  private final KafkaConfig config;
  private final Collection<BackedStoreRef<?, ?>> backedStoreRefs;
  private final Collection<LiveStoreRef<?, ?>> liveStoreRefs;

  private Collection<ResolvedBackedStoreRef<?, ?>> resolvedBackedStoreRefs;
  private Map<ResolvedBackedStoreRef<?, ?>, InternalKvStore<?, ?>> backedStores;
  private Map<LiveStoreRef<?, ?>, InternalKvStore<?, ?>> liveStores;
  private Map<BackedStoreRef<?, ?>, ResolvedBackedStoreRef<?, ?>> originalToResolvedStoreRefs = new HashMap<>();
  private Map<BackedStoreRef<?, ?>, BackedStore<?, ?>> apiBackedStores = new ConcurrentHashMap<>();
  private Map<LiveStoreRef<?, ?>, LiveStore<?, ?>> apiLiveStores = new ConcurrentHashMap<>();
  private RestoreThreadContainer restoreThreadContainer;
  private GlobalThreadContainer globalThreadContainer;
  private Producer<byte[], byte[]> producer;
  private boolean initialized = false;

  @Builder
  private KafkaStateConnector(StoreFactory storeFactory, int numPartitions, short replicationFactor,
      AdminClientFactory adminClientFactory, ConsumerFactory consumerFactory,
      ProducerFactory producerFactory, @NonNull KafkaConfig config,
      @Nullable Collection<BackedStoreRef<?, ?>> backedStoreRefs,
      @Nullable Collection<LiveStoreRef<?, ?>> liveStoreRefs) {
    String stateDir = config.getStateDir();
    String applicationId = config.getApplicationId();
    Objects
        .requireNonNull(applicationId, "ApplicationId required for " + getClass().getSimpleName());
    Objects.requireNonNull(stateDir, "StateDir required for " + getClass().getSimpleName());

    this.storeFactory = orDefault(storeFactory,
        () -> StoreFactory.defaultFactory(config.getStateDir()));
    this.numPartitions = numPartitions;
    this.replicationFactor = replicationFactor;
    this.adminClientFactory = orDefault(adminClientFactory, AdminClientFactory::defaultFactory);
    this.consumerFactory = orDefault(consumerFactory, ConsumerFactory::defaultFactory);
    this.producerFactory = orDefault(producerFactory, ProducerFactory::defaultFactory);
    this.config = config;
    this.backedStoreRefs = orDefault(backedStoreRefs, LinkedList::new);
    this.liveStoreRefs = orDefault(liveStoreRefs, LinkedList::new);
  }

  private <T> T orDefault(@Nullable T obj, Supplier<T> supplier) {
    return Optional.ofNullable(obj).orElseGet(supplier);
  }

  @PostConstruct
  public void initialize() throws InterruptedException, ExecutionException {

    log.info("Initializing {} with {} {} and {} {}", getClass().getSimpleName(),
        backedStoreRefs.size(), BackedStoreRef.class.getSimpleName(),
        liveStoreRefs.size(), LiveStoreRef.class.getSimpleName());

    ByteArraySerializer serializer = new ByteArraySerializer();
    producer = producerFactory.create(config.getProducer(), serializer, serializer);

    resolvedBackedStoreRefs = backedStoreRefs.stream()
        .map(ref -> {
          ResolvedBackedStoreRef<?, ?> resolved = new ResolvedBackedStoreRef<>(backedTopicName(ref),
              ref.getKeySerde(), ref.getValueSerde(), ref.getType());
          originalToResolvedStoreRefs.put(ref, resolved);
          return resolved;
        })
        .collect(Collectors.toList());

    initializeTopics();

    backedStores = new HashMap<>();
    for (ResolvedBackedStoreRef<?, ?> ref : resolvedBackedStoreRefs) {
      KvStore<?, ?> kvStore = storeFactory.create(getStoreConfig(ref));
      if (!InternalKvStore.class.isAssignableFrom(kvStore.getClass())) {
        throw new IllegalStateException(String
            .format("Store created from %s must implement %s. Store for ref does not: %s",
                storeFactory.getClass().getSimpleName(), InternalKvStore.class.getSimpleName(),
                ref));
      }
      ((InternalKvStore<?, ?>) kvStore).clear();
      backedStores.put(ref, (InternalKvStore<?, ?>) kvStore);
    }
    CompletableFuture<Void> restoreThreadInitComplete;
    if (!backedStores.isEmpty()) {
      restoreThreadContainer = new RestoreThreadContainer(config, backedStores, consumerFactory);
      restoreThreadInitComplete = restoreThreadContainer.initialize();
    } else {
      restoreThreadInitComplete = CompletableFuture.completedFuture(null);
    }

    liveStores = new HashMap<>();
    for (LiveStoreRef<?, ?> ref : liveStoreRefs) {
      KvStore<?, ?> kvStore = storeFactory.create(getStoreConfig(ref));
      if (!InternalKvStore.class.isAssignableFrom(kvStore.getClass())) {
        throw new IllegalStateException(String
            .format("Store created from %s must implement %s. Store for ref does not: %s",
                storeFactory.getClass().getSimpleName(), InternalKvStore.class.getSimpleName(),
                ref));
      }
      ((InternalKvStore<?, ?>) kvStore).clear();
      liveStores.put(ref, (InternalKvStore<?, ?>) kvStore);
    }
    CompletableFuture<Void> globalThreadInitComplete;
    if (!liveStores.isEmpty()) {
      globalThreadContainer = new GlobalThreadContainer(config, liveStores, consumerFactory);
      globalThreadInitComplete = globalThreadContainer.initialize();
    } else {
      globalThreadInitComplete = CompletableFuture.completedFuture(null);
    }

    CompletableFuture.allOf(restoreThreadInitComplete, globalThreadInitComplete).get();
    this.initialized = true;
  }

  @SuppressWarnings("unchecked")
  public <K, V> KvStore<K, V> store(BackedStoreRef<K, V> storeRef) {
    assertInitialized();

    return (KvStore<K, V>) apiBackedStores.computeIfAbsent(storeRef, r -> {
      Optional<ResolvedBackedStoreRef<?, ?>> resolvedRef = Optional.of(r)
          .map(originalToResolvedStoreRefs::get);

      InternalKvStore<?, ?> rawUpdateKvStore = resolvedRef
          .map(backedStores::get)
          .orElseThrow(
              () -> new IllegalStateException("Could not find created store for ref " + r));

      return new BackedStore<>(rawUpdateKvStore, resolvedRef.get().getTopicName(), producer);
    });
  }

  @SuppressWarnings("unchecked")
  public <K, V> ReadOnlyKvStore<K, V> store(LiveStoreRef<K, V> storeRef) {
    assertInitialized();

    return (ReadOnlyKvStore<K, V>) apiLiveStores.computeIfAbsent(storeRef, r -> {

      InternalKvStore<?, ?> rawUpdateKvStore = Optional.of(r)
          .map(liveStores::get)
          .orElseThrow(
              () -> new IllegalStateException("Could not find created store for ref " + r));

      return new LiveStore<>(rawUpdateKvStore);
    });
  }

  private void assertInitialized() {
    if (!initialized) {
      throw new IllegalStateException("Initialization has not been completed");
    }
  }

  private <K, V> StoreConfig<K, V> getStoreConfig(LiveStoreRef<K, V> ref) {
    return StoreConfig.<K, V>builder()
        .name(ref.getTopicName())
        .keySerde(new TopicSpecificSerde<>(ref.getTopicName(), ref.getKeySerde()))
        .valueSerde(new TopicSpecificSerde<>(ref.getTopicName(), ref.getValueSerde()))
        .build();
  }

  private <K, V> StoreConfig<K, V> getStoreConfig(ResolvedBackedStoreRef<K, V> ref) {
    return StoreConfig.<K, V>builder()
        .name(ref.getTopicName())
        .keySerde(new TopicSpecificSerde<>(ref.getTopicName(), ref.getKeySerde()))
        .valueSerde(new TopicSpecificSerde<>(ref.getTopicName(), ref.getValueSerde()))
        .build();
  }

  private void initializeTopics() {
    List<String> liveTopics = liveStoreRefs.stream().map(LiveStoreRef::getTopicName)
        .collect(Collectors.toList());
    List<String> backedInternalTopics = resolvedBackedStoreRefs.stream()
        .filter(r -> r.getType() == BackedTopicType.INTERNAL)
        .map(ResolvedBackedStoreRef::getTopicName)
        .collect(Collectors.toList());
    List<String> backedPublicTopics = resolvedBackedStoreRefs.stream()
        .filter(r -> r.getType() == BackedTopicType.PUBLIC)
        .map(ResolvedBackedStoreRef::getTopicName)
        .collect(Collectors.toList());

    List<String> allTopics = new ArrayList<>(liveTopics.size()
        + backedInternalTopics.size()
        + backedPublicTopics.size());
    allTopics.addAll(liveTopics);
    allTopics.addAll(backedInternalTopics);
    allTopics.addAll(backedPublicTopics);

    if (allTopics.isEmpty()) {
      return;
    }

    try (AdminClient client = adminClientFactory.create(config.getAdmin())) {

      log.info("Verifying cluster has required topics");
      Set<String> existingTopics = client.listTopics().names().get();
//      DescribeTopicsResult result = client.describeTopics(allTopics);
//      Map<String, TopicDescription> existingTopics;
//      existingTopics = new HashMap<>(result.all().get());

      createMissingBackedInternalTopics(client, backedInternalTopics, existingTopics);
      verifyRequiredTopics(client, allTopics);
      log.info("Verified cluster has correct topics");

    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  private void verifyRequiredTopics(AdminClient client, List<String> requiredTopics)
      throws ExecutionException, InterruptedException {
    if (requiredTopics.isEmpty()) {
      return;
    }

    Set<String> existingTopics = client.listTopics().names().get();
    if (!existingTopics.containsAll(requiredTopics)) {
      Set<String> missingTopics = new HashSet<>(requiredTopics);
      missingTopics.removeAll(existingTopics);
      throw new IllegalStateException("Missing required topics " + missingTopics);
    }
  }

  private void createMissingBackedInternalTopics(AdminClient client, List<String> backedTopics,
      Set<String> existingTopics)
      throws InterruptedException, ExecutionException {

    CreateTopicsResult createResult;
    Set<String> createTopics = new HashSet<>(backedTopics);
    createTopics.removeAll(existingTopics);

    if (createTopics.isEmpty()) {
      log.debug("No need to create any topics");
      return;
    }

    List<NewTopic> newTopicList = createTopics.stream()
        .map(t -> new NewTopic(t, numPartitions, replicationFactor))
        .collect(Collectors.toList());
    log.info("Creating missing topics {}", newTopicList);
    createResult = client.createTopics(newTopicList);
    Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
    createResult.all().get();

    // set new topics to "compact"
    ConfigEntry compactConfig = new ConfigEntry("cleanup.policy", "compact");
    AlterConfigOp configOp = new AlterConfigOp(compactConfig, OpType.SET);
    for (String createTopic : createTopics) {
      configs.put(new ConfigResource(Type.TOPIC, createTopic), Collections.singletonList(configOp));
    }
    log.info("Modifying all newly created topics with cleanup.policy: compact: {}", configs);
    client.incrementalAlterConfigs(configs).all().get();
  }

  private String backedTopicName(BackedStoreRef<?, ?> ref) {
    if (ref.getType() == BackedTopicType.INTERNAL) {
      return String.format("%s-%s-changelog", config.getApplicationId(), ref.getName());
    } else if (ref.getType() == BackedTopicType.PUBLIC) {
      return ref.getName();
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown type of %s: %s", ref.getClass().getSimpleName(), ref.getType()));
    }
  }
}
