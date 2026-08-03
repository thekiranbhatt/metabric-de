CREATE TABLE IF NOT EXISTS dim_subtype (
    subtype_key         SERIAL PRIMARY KEY,
    pam50_subtype       VARCHAR NOT NULL,
    integrative_cluster VARCHAR NOT NULL,
    three_gene_subtype  VARCHAR NOT NULL,
    CONSTRAINT uq_molecular_combination UNIQUE (pam50_subtype, integrative_cluster, three_gene_subtype)
);