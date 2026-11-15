package com.payflow.transactions.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.transactions.domain.OutboxEvent;
import com.payflow.transactions.domain.Transaction;
import com.payflow.transactions.domain.TransactionStatus;
import com.payflow.transactions.event.TransactionEvent;
import com.payflow.transactions.repository.OutboxEventRepository;
import com.payflow.transactions.repository.TransactionRepository;
import java.time.Instant;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Marks a transaction COMPLETED and records its outbox event in ONE database
 * transaction: either both commit or neither does. Lives in its own bean so the
 * {@code @Transactional} proxy applies when {@link TransactionService} calls it.
 */
@Service
public class TransactionCompletionService {

    public static final String AGGREGATE_TYPE = "Transaction";
    public static final String EVENT_TYPE = "transaction.completed";

    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public TransactionCompletionService(TransactionRepository transactionRepository,
                                        OutboxEventRepository outboxRepository,
                                        ObjectMapper objectMapper) {
        this.transactionRepository = transactionRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Transaction complete(Transaction txn, String currency) {
        txn.setCurrency(currency);
        txn.setStatus(TransactionStatus.COMPLETED);
        Transaction saved = transactionRepository.save(txn);

        TransactionEvent event = new TransactionEvent(
                saved.getId(), saved.getFromAccount(), saved.getToAccount(),
                saved.getAmount(), saved.getCurrency(),
                saved.getDescription(), saved.getMerchant(), Instant.now());

        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateType(AGGREGATE_TYPE);
        outbox.setAggregateId(saved.getId().toString());
        outbox.setEventType(EVENT_TYPE);
        try {
            outbox.setPayload(objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            // Roll the whole transaction back rather than complete without an event.
            throw new IllegalStateException("Could not serialize outbox payload", e);
        }
        outboxRepository.save(outbox);
        return saved;
    }
}
