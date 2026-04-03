-- RAG schema for Neon PostgreSQL + pgvector
-- Run once against your Neon project to set up tables and indexes.
--
-- Usage:
--   psql "$RAG_DATABASE_URL" -f rag/schema.sql

CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS rag;

-- Parent table: one row per ingested document (filing, transcript, thesis)
CREATE TABLE IF NOT EXISTS rag.documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ticker VARCHAR(10) NOT NULL,
    sector VARCHAR(50),
    doc_type VARCHAR(50) NOT NULL,      -- '10-K', '10-Q', 'earnings_transcript', 'thesis'
    source VARCHAR(50) NOT NULL,         -- 'sec_edgar', 'fmp', 'alpha_engine'
    filed_date DATE NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    title TEXT,
    url TEXT,
    UNIQUE(ticker, doc_type, filed_date, source)
);

-- Child table: embedded chunks with section labels
CREATE TABLE IF NOT EXISTS rag.chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID REFERENCES rag.documents(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    section_label VARCHAR(100),          -- 'Risk Factors', 'MD&A', 'prepared_remarks', 'qa_session', etc.
    embedding vector(512),               -- Voyage voyage-3-lite dimension
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- HNSW index for fast cosine similarity search
CREATE INDEX IF NOT EXISTS chunks_embedding_hnsw
    ON rag.chunks USING hnsw (embedding vector_cosine_ops);

-- Metadata filtering indexes
CREATE INDEX IF NOT EXISTS documents_ticker_type_date
    ON rag.documents (ticker, doc_type, filed_date);
CREATE INDEX IF NOT EXISTS documents_sector
    ON rag.documents (sector);
CREATE INDEX IF NOT EXISTS chunks_document_id
    ON rag.chunks (document_id);
