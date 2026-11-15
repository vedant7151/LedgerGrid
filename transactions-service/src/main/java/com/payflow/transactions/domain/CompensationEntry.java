package com.payflow.transactions.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A refund that could not be completed inline: the sender was debited, the receiver
 * could not be credited, and the compensating refund to the sender ALSO failed. The
 * money is in limbo until {@code CompensationRetryJob} manages to refund it. The
 * sender account and amount are read from the referenced transaction.
 */
@Entity
@Table(name = "compensation_queue", indexes = @Index(name = "idx_compensation_status", columnList = "status"))
@Getter
@Setter
@NoArgsConstructor
public class CompensationEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private UUID transactionId;

    /** Why the latest attempt failed (most recent error message). */
    @Column(nullable = false, length = 1000)
    private String reason;

    /** Number of retries made by the job (the failed inline attempt is not counted). */
    @Column(nullable = false)
    private int attemptCount = 0;

    /** Time of the most recent attempt, initially the failed inline refund. Drives the backoff. */
    @Column(nullable = false)
    private Instant lastAttemptedAt;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private CompensationStatus status = CompensationStatus.PENDING;
}
