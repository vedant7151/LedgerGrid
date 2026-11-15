package com.payflow.transactions.repository;

import com.payflow.transactions.domain.CompensationEntry;
import com.payflow.transactions.domain.CompensationStatus;
import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CompensationEntryRepository extends JpaRepository<CompensationEntry, UUID> {

    List<CompensationEntry> findByStatusOrderByLastAttemptedAtAsc(CompensationStatus status);
}
