package org.brokencircuits.householdfinance.stores;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.brokencircuits.householdfinance.stores.rocks.RocksDbStore;

public interface StoreFactory {

  <K, V> KvStore<K, V> create(StoreConfig<K, V> config);

  static StoreFactory defaultFactory(String stateDir) {
    Map<String, KvStore<?, ?>> createdStores = new ConcurrentHashMap<>();
    return new StoreFactory() {
      @Override
      public <K, V> KvStore<K, V> create(StoreConfig<K, V> config) {
        if (createdStores.containsKey(config.getName())) {
          throw new IllegalStateException(String.format("Cannot create more than 1 store with "
              + "name %s", config.getName()));
        }
        RocksDbStore<K, V> store = new RocksDbStore<>(new File(stateDir), config.getName(),
            config.getKeySerde(), config.getValueSerde());
        store.initialize();

        createdStores.compute(config.getName(), (name, s) -> {
          if (s != null) {
            throw new IllegalStateException(
                String.format("Cannot create more than 1 store with name %s", name));
          }
          return store;
        });
        return store;
      }
    };
  }
}
