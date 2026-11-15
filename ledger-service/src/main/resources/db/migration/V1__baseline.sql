-- ledger_db baseline (Phase 9). Reflects the schema Hibernate generated for the current
-- entity (including Phase 2b anomaly columns and the Phase 6 chain_index unique constraint);
-- Hibernate now only VALIDATES against it (ddl-auto: validate).

CREATE TABLE ledger_entries (
    id              uuid                        NOT NULL,
    amount          numeric(38,2)               NOT NULL,
    anomaly_reason  varchar(255),               -- Phase 2b: short machine-readable reason, not part of the hash
    category        varchar(255),
    chain_index     bigint,
    currency        varchar(255)                NOT NULL,
    description     varchar(255),
    entry_hash      varchar(64),
    from_account    uuid                        NOT NULL,
    is_anomalous    boolean                     NOT NULL DEFAULT false,  -- Phase 2b
    merchant        varchar(255),
    previous_hash   varchar(64),
    recorded_at     timestamp(6) with time zone NOT NULL,
    to_account      uuid                        NOT NULL,
    transaction_id  uuid                        NOT NULL,
    CONSTRAINT ledger_entries_pkey PRIMARY KEY (id),
    -- Phase 6: no two entries may claim the same position in the hash chain.
    CONSTRAINT uk_ledger_entries_chain_index UNIQUE (chain_index),
    -- A redelivered event can never be recorded twice.
    CONSTRAINT uk_ledger_entries_transaction_id UNIQUE (transaction_id)
);
