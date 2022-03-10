package org.brokencircuits.householdfinance.kafka.serde;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

public interface AvroSerdeFactory {
  <T extends SpecificRecord> Serde<T> keySerde();
  <T extends SpecificRecord> Serde<T> valueSerde();

  static AvroSerdeFactory testFactory(MockSchemaRegistryClient client) {
    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:65535");
    return new AvroSerdeFactory() {
      @Override
      public <T extends SpecificRecord> Serde<T> keySerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>(client);
        serde.configure(serdeConfig, true);
        return serde;
      }

      @Override
      public <T extends SpecificRecord> Serde<T> valueSerde() {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>(client);
        serde.configure(serdeConfig, false);
        return serde;
      }
    };
  }
}
