package org.brokencircuits.householdfinance.kafka.messaging;

import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

@Component
@Import(UnderlyingKafkaConfig.class)
public class KafkaConfig {

  @Autowired
  private UnderlyingKafkaConfig config;

  public String getSchemaRegistryUrl() {
    String url = config.getSchemaRegistryUrl();
    Objects.requireNonNull(url, "SchemaRegistryUrl is required");
    return url;
  }

  public String getApplicationId() {
    String id = Optional.ofNullable(config.getApplicationId())
        .orElse(config.getStreams().get("application.id"));
    Objects.requireNonNull(id, "application.id is required");
    return id;
  }

  public String getStateDir() {
    String dir = Optional.ofNullable(config.getStateDir())
        .orElse(config.getStreams().get("state.dir"));
    Objects.requireNonNull(dir, "state.dir is required");
    return dir;
  }

  public Properties getConsumer() {
    Properties p = new Properties();
    p.putAll(config.getGlobal());
    p.putAll(config.getConsumer());
    return p;
  }

  public Properties getProducer() {
    Properties p = new Properties();
    p.putAll(config.getGlobal());
    p.putAll(config.getProducer());
    return p;
  }

  public Properties getAdmin() {
    Properties p = new Properties();
    p.putAll(config.getGlobal());
    p.putAll(config.getAdmin());
    return p;
  }

  public Properties getStreams() {
    Properties p = new Properties();
    p.putAll(config.getGlobal());
    p.putAll(config.getStreams());
    return p;
  }


}
