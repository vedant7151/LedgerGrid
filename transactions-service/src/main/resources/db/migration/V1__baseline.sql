-- transactions_db baseline (Phase 9). Reflects the schema Hibernate generated for the
-- current entities (including Phase 5 outbox_event and Phase 7 compensation_queue);
-- Hibernate now only VALIDATES against it (ddl-auto: validate).

CREATE TABLE transactions (
    id               uuid                        NOT NULL,
    amount           numeric(38,2)               NOT NULL,
    created_at       timestamp(6) with time zone NOT NULL,
    currency         varchar(255)                NOT NULL,
    description      varchar(255),
    failure_reason   varchar(255),
    from_account     uuid                        NOT NULL,
    idempotency_key  varchar(255),
    merchant         varchar(255),
    status           varchar(255)                NOT NULL,
    to_account       uuid                        NOT NULL,
    CONSTRAINT transactions_pkey PRIMARY KEY (id),
    CONSTRAINT uk_transactions_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT transactions_status_check CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED'))
);

-- Phase 5: transactional outbox. Rows are written in the same DB transaction as the
-- COMPLETED status and drained to Kafka by OutboxPublisher.
CREATE TABLE outbox_event (
    id              uuid                        NOT NULL,
    aggregate_id    varchar(255)                NOT NULL,
    aggregate_type  varchar(255)                NOT NULL,
    created_at      timestamp(6) with time zone NOT NULL,
    event_type      varchar(255)                NOT NULL,
    payload         text                        NOT NULL,
    sent_at         timestamp(6) with time zone,
    status          varchar(255)                NOT NULL,
    CONSTRAINT outbox_event_pkey PRIMARY KEY (id),
    CONSTRAINT outbox_event_status_check CHECK (status IN ('PENDING', 'SENT'))
);
CREATE INDEX idx_outbox_status_created ON outbox_event (status, created_at);

-- Phase 7: refunds that failed inline and are retried by CompensationRetryJob.
CREATE TABLE compensation_queue (
    id                 uuid                        NOT NULL,
    attempt_count      integer                     NOT NULL,
    last_attempted_at  timestamp(6) with time zone NOT NULL,
    reason             varchar(1000)               NOT NULL,
    status             varchar(255)                NOT NULL,
    transaction_id     uuid                        NOT NULL,
    CONSTRAINT compensation_queue_pkey PRIMARY KEY (id),
    CONSTRAINT compensation_queue_status_check CHECK (status IN ('PENDING', 'RESOLVED', 'FAILED_PERMANENTLY'))
);
CREATE INDEX idx_compensation_status ON compensation_queue (status);
