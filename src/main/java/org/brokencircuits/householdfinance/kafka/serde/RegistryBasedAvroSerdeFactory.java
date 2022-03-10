package org.brokencircuits.householdfinance.kafka.serde;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;

@RequiredArgsConstructor
public class RegistryBasedAvroSerdeFactory implements
    AvroSerdeFactory {

  private final String schemaRegistryUrl;

  public <T extends SpecificRecord> Serde<T> keySerde() {
    final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    serde.configure(serdeConfig, true);
    return serde;
  }

  public <T extends SpecificRecord> Serde<T> valueSerde() {
    final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
    final Map<String, String> serdeConfig = Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
    serde.configure(serdeConfig, false);
    return serde;
  }

}
