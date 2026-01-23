-- Licensed under the Apache License, Version 2.0
-- Branching Metastore Database Schema
-- PostgreSQL with JSONB for complex types

-- Branches table: core branching metadata
CREATE TABLE IF NOT EXISTS branches (
    branch_id VARCHAR(255) PRIMARY KEY,
    parent_id VARCHAR(255),
    head_snapshot_id VARCHAR(255),
    branch_type VARCHAR(50) NOT NULL,
    comment TEXT,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_parent_branch FOREIGN KEY (parent_id) REFERENCES branches(branch_id) ON DELETE SET NULL
);

-- Index for branch type queries
CREATE INDEX idx_branches_type ON branches(branch_type);

-- Databases table: branch-scoped database metadata
CREATE TABLE IF NOT EXISTS databases (
    id BIGSERIAL PRIMARY KEY,
    branch_id VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    location TEXT,
    comment TEXT,
    properties JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_databases_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE,
    CONSTRAINT uq_databases_branch_name UNIQUE (branch_id, database_name)
);

-- Index for database listing
CREATE INDEX idx_databases_branch ON databases(branch_id);

-- Tables table: branch-scoped table metadata with JSONB for schema
CREATE TABLE IF NOT EXISTS tables (
    id BIGSERIAL PRIMARY KEY,
    branch_id VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    table_type VARCHAR(50) NOT NULL,
    storage JSONB NOT NULL,
    data_columns JSONB NOT NULL,
    partition_columns JSONB DEFAULT '[]',
    parameters JSONB DEFAULT '{}',
    view_original_text TEXT,
    view_expanded_text TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_tables_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE,
    CONSTRAINT fk_tables_database FOREIGN KEY (database_name, branch_id) REFERENCES databases(database_name, branch_id) ON DELETE CASCADE,
    CONSTRAINT uq_tables_branch_db_name UNIQUE (branch_id, database_name, table_name)
);

-- Indexes for table listing and queries
CREATE INDEX idx_tables_branch_db ON tables(branch_id, database_name);
CREATE INDEX idx_tables_branch_db_name ON tables(branch_id, database_name, table_name);

-- Partitions table: branch-scoped partition metadata (for Hive-style tables)
CREATE TABLE IF NOT EXISTS partitions (
    id BIGSERIAL PRIMARY KEY,
    branch_id VARCHAR(255) NOT NULL,
    database_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    partition_name VARCHAR(767) NOT NULL,
    storage JSONB NOT NULL,
    data_columns JSONB NOT NULL,
    parameters JSONB DEFAULT '{}',
    column_statistics JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_partitions_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE,
    CONSTRAINT fk_partitions_table FOREIGN KEY (branch_id, database_name, table_name)
        REFERENCES tables(branch_id, database_name, table_name) ON DELETE CASCADE,
    CONSTRAINT uq_partitions_branch_table_name UNIQUE (branch_id, database_name, table_name, partition_name)
);

-- Indexes for partition queries
CREATE INDEX idx_partitions_branch_table ON partitions(branch_id, database_name, table_name);
CREATE INDEX idx_partitions_branch_table_name ON partitions(branch_id, database_name, table_name, partition_name);

-- Snapshots table: for commit history and time travel
CREATE TABLE IF NOT EXISTS snapshots (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id VARCHAR(255) UNIQUE NOT NULL,
    branch_id VARCHAR(255) NOT NULL,
    parent_snapshot_id VARCHAR(255),
    commit_message TEXT,
    committer VARCHAR(255),
    commit_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_snapshots_branch FOREIGN KEY (branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE,
    CONSTRAINT fk_snapshots_parent FOREIGN KEY (parent_snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE SET NULL
);

-- Index for snapshot queries by branch
CREATE INDEX idx_snapshots_branch ON snapshots(branch_id);
CREATE INDEX idx_snapshots_commit_time ON snapshots(commit_time DESC);

-- Merge history table: for tracking branch merges
CREATE TABLE IF NOT EXISTS merge_history (
    id BIGSERIAL PRIMARY KEY,
    source_branch_id VARCHAR(255) NOT NULL,
    target_branch_id VARCHAR(255) NOT NULL,
    snapshot_id VARCHAR(255) NOT NULL,
    merge_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    commit_message TEXT,
    merger VARCHAR(255),
    CONSTRAINT fk_merge_history_source FOREIGN KEY (source_branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE,
    CONSTRAINT fk_merge_history_target FOREIGN KEY (target_branch_id) REFERENCES branches(branch_id) ON DELETE CASCADE,
    CONSTRAINT fk_merge_history_snapshot FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE
);

-- Index for merge history queries
CREATE INDEX idx_merge_history_target ON merge_history(target_branch_id, merge_time DESC);

-- Insert main branch
INSERT INTO branches (branch_id, parent_id, branch_type, comment, properties)
VALUES ('main', NULL, 'MAIN', 'Main branch', '{}')
ON CONFLICT (branch_id) DO NOTHING;
