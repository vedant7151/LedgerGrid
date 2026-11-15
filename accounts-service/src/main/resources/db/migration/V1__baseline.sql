-- accounts_db baseline (Phase 9). Reflects the schema Hibernate generated for the
-- current entities; Hibernate now only VALIDATES against it (ddl-auto: validate).

CREATE TABLE accounts (
    id          uuid                        NOT NULL,
    balance     numeric(38,2)               NOT NULL,
    created_at  timestamp(6) with time zone NOT NULL,
    currency    varchar(255)                NOT NULL,
    owner_name  varchar(255)                NOT NULL,
    version     bigint,                     -- JPA @Version (optimistic locking)
    CONSTRAINT accounts_pkey PRIMARY KEY (id)
);
