package org.brokencircuits.householdfinance.persistence;

import org.brokencircuits.householdfinance.domain.Transaction;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface InventoryRepository
    extends CrudRepository<Transaction, String> {
}
