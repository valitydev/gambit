CREATE SCHEMA IF NOT EXISTS gbt;

CREATE TABLE gbt.data_set_info
(
    id          serial       NOT NULL,
    name        varchar         NOT NULL,
    headers     varchar         NOT NULL,

    CONSTRAINT data_set_info_pkey PRIMARY KEY (id),
    CONSTRAINT data_set_info_uniq UNIQUE (name)
);

CREATE TABLE gbt.data
(
    id                  bigserial       NOT NULL,
    data_set_info_id     integer         NOT NULL,
    values              varchar         NOT NULL,
    values_hash         varchar         NOT NULL,

    CONSTRAINT data_pkey PRIMARY KEY (id)
);
CREATE INDEX data_set_info_idx ON gbt.data USING btree (data_set_info_id);
CREATE INDEX values_hash_idx ON gbt.data USING btree (values_hash);

CREATE TABLE gbt.data_lookup
(
    id                  bigserial       NOT NULL,
    data_set_info_id     integer         NOT NULL,
    data_id             bigint          NOT NULL,
    hash                integer         NOT NULL,

    CONSTRAINT data_lookup_pkey PRIMARY KEY (id),
    CONSTRAINT data_lookup_uniq UNIQUE (data_set_info_id, hash)
);