"""RAG library — semantic retrieval over SEC filings, earnings transcripts, and theses."""

# Auto-load .env so RAG_DATABASE_URL and VOYAGE_API_KEY are available
# whether run from CLI, Lambda (already in env), or imported in tests.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed (e.g. Lambda) — env vars set externally
