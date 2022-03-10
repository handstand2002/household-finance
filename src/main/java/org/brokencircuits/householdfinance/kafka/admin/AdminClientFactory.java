package org.brokencircuits.householdfinance.kafka.admin;

import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;

public interface AdminClientFactory {
  AdminClient create(Properties props);

  static AdminClientFactory defaultFactory() {
    return AdminClient::create;
  }
}
