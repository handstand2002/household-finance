package org.brokencircuits.householdfinance.domain;

import java.math.BigDecimal;
import java.time.Instant;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Transaction {

  @Id
  @Column(nullable = false)
  private String id;
  @Column(precision = 20, scale = 2, nullable = false)
  private BigDecimal amount;
  @Column(nullable = false)
  private String source;
  @Column(nullable = false)
  private String account;
  @Column(nullable = false)
  private Instant timestamp = Instant.now();
}