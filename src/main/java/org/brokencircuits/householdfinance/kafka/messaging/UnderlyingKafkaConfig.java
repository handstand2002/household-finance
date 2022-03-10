package org.brokencircuits.householdfinance.kafka.messaging;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "kafka")
public class UnderlyingKafkaConfig {

  private Map<String, String> global = new HashMap<>();
  private Map<String, String> consumer = new HashMap<>();
  private Map<String, String> producer = new HashMap<>();
  private Map<String, String> admin = new HashMap<>();
  private Map<String, String> streams = new HashMap<>();
  private String schemaRegistryUrl;
  private String applicationId;
  private String stateDir;
}
