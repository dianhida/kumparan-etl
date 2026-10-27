#!/usr/bin/env python3
"""Generate deterministic dummy data for articles table (2016-2026)."""

import sys
import os
import random
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row


# Fixed seed for reproducibility
SEED = 42
random.seed(SEED)

# Configuration
TOTAL_ARTICLES_2016_2025 = 10000
TOTAL_ARTICLES_2026 = 1000
BATCH_SIZE = 500

# Sample data for generating realistic content
TITLE_TEMPLATES = [
    "How to {verb} {noun} in {year}",
    "The Ultimate Guide to {noun}",
    "{number} Ways to {verb} {noun}",
    "Why {noun} Matters for {audience}",
    "Understanding {noun}: A Deep Dive",
    "{noun} vs {noun2}: Which is Better?",
    "The Future of {noun} in {year}",
    "Top {number} {noun} Trends for {year}",
    "Getting Started with {noun}",
    "Advanced {noun} Techniques",
]

VERBS = ["build", "create", "optimize", "learn", "master", "deploy", "scale", "debug", "test", "design"]
NOUNS = ["APIs", "databases", "microservices", "containers", "Kubernetes", "Python", "JavaScript", "React", "Node.js", "Go", "Rust", "Docker", "CI/CD", "monitoring", "logging", "security", "performance", "architecture", "cloud", "serverless"]
NOUNS2 = ["REST", "GraphQL", "gRPC", "SQL", "NoSQL", "PostgreSQL", "MongoDB", "Redis", "Kafka", "RabbitMQ"]
AUDIENCES = ["developers", "engineers", "teams", "startups", "enterprises", "beginners", "experts"]
NUMBERS = [3, 5, 7, 10, 15, 20, 25, 30, 50, 100]

CONTENT_PARAGRAPHS = [
    "In this article, we explore the fundamentals of {topic} and how it applies to modern software development.",
    "Understanding {topic} is crucial for building scalable and maintainable systems.",
    "We'll cover best practices, common pitfalls, and real-world examples.",
    "By the end of this guide, you'll have a solid foundation in {topic}.",
    "Let's dive into the key concepts and practical applications.",
]

AUTHORS = list(range(1, 51))  # 50 authors


def get_db_connection():
    """Get PostgreSQL connection from environment."""
    load_dotenv()
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "kumparan"),
        password=os.getenv("POSTGRES_PASSWORD", "kumparan"),
        dbname=os.getenv("POSTGRES_DB", "kumparan"),
        connect_timeout=10,
        row_factory=dict_row,
    )


def check_existing_data(conn):
    """Check if articles table already has data."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) as count FROM public.articles")
        result = cur.fetchone()
        return result['count']


def generate_title():
    """Generate a realistic article title."""
    template = random.choice(TITLE_TEMPLATES)
    return template.format(
        verb=random.choice(VERBS),
        noun=random.choice(NOUNS),
        noun2=random.choice(NOUNS2),
        number=random.choice(NUMBERS),
        audience=random.choice(AUDIENCES),
        year=random.randint(2016, 2026),
    )


def generate_content():
    """Generate realistic article content."""
    topic = random.choice(NOUNS)
    num_paragraphs = random.randint(3, 8)
    paragraphs = []
    for _ in range(num_paragraphs):
        p = random.choice(CONTENT_PARAGRAPHS).format(topic=topic)
        paragraphs.append(p)
    return "\n\n".join(paragraphs)


def generate_timestamp_for_year(year):
    """Generate a random timezone-aware timestamp within a given year."""
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def generate_article_data(year_range, is_2026=False):
    """Generate a single article record."""
    if is_2026:
        year = 2026
    else:
        year = random.randint(year_range[0], year_range[1])

    created_at = generate_timestamp_for_year(year)

    # updated_at >= created_at, within same year or slightly later
    max_updated = datetime(min(year + 1, 2026), 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    if max_updated <= created_at:
        max_updated = created_at + timedelta(days=365)
    updated_delta = random.randint(0, int((max_updated - created_at).total_seconds()))
    updated_at = created_at + timedelta(seconds=updated_delta)

    # published_at: 80% published, 20% unpublished (NULL)
    if random.random() < 0.8:
        # Published after or at created_at
        pub_delta = random.randint(0, int((updated_at - created_at).total_seconds()))
        published_at = created_at + timedelta(seconds=pub_delta)
    else:
        published_at = None

    return {
        'title': generate_title(),
        'content': generate_content(),
        'published_at': published_at,
        'author_id': random.choice(AUTHORS),
        'created_at': created_at,
        'updated_at': updated_at,
        'deleted_at': None,  # Always NULL for initial dataset
    }


def insert_batch(conn, articles):
    """Insert a batch of articles using parameterized query."""
    with conn.cursor() as cur:
        query = """
            INSERT INTO public.articles (title, content, published_at, author_id, created_at, updated_at, deleted_at)
            VALUES (%(title)s, %(content)s, %(published_at)s, %(author_id)s, %(created_at)s, %(updated_at)s, %(deleted_at)s)
        """
        cur.executemany(query, articles)
    conn.commit()


def generate_and_insert(conn, count, year_range, is_2026=False, description=""):
    """Generate and insert articles in batches."""
    print(f"  Generating {count} articles for {description}...")
    total_inserted = 0
    batch = []

    for i in range(count):
        article = generate_article_data(year_range, is_2026)
        batch.append(article)

        if len(batch) >= BATCH_SIZE:
            insert_batch(conn, batch)
            total_inserted += len(batch)
            print(f"    Inserted {total_inserted}/{count}...")
            batch = []

    # Insert remaining
    if batch:
        insert_batch(conn, batch)
        total_inserted += len(batch)

    print(f"  Completed: {total_inserted} articles inserted")
    return total_inserted


def main():
    print("=" * 60)
    print("Phase 2: Articles Dummy Data Generator")
    print("=" * 60)

    # Connect to database
    try:
        conn = get_db_connection()
        print("✓ Connected to PostgreSQL")
    except Exception as e:
        print(f"✗ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

    try:
        # Check existing data
        existing_count = check_existing_data(conn)
        if existing_count > 0:
            print(f"\n⚠ Articles table already contains {existing_count} records.")
            print("  To regenerate data, you must first truncate the table:")
            print("  TRUNCATE TABLE articles RESTART IDENTITY;")
            print("\n  Exiting without changes to prevent duplicates.")
            conn.close()
            sys.exit(1)

        print(f"\nStarting data generation (seed={SEED})...")
        print(f"  Target: ~{TOTAL_ARTICLES_2016_2025} articles (2016-2025)")
        print(f"  Target: ~{TOTAL_ARTICLES_2026} articles (2026)")

        # Generate 2016-2025 articles
        count_2016_2025 = generate_and_insert(
            conn, TOTAL_ARTICLES_2016_2025, (2016, 2025), False, "2016-2025"
        )

        # Generate 2026 articles
        count_2026 = generate_and_insert(
            conn, TOTAL_ARTICLES_2026, (2026, 2026), True, "2026"
        )

        total = count_2016_2025 + count_2026
        print(f"\n✓ Data generation complete: {total} total articles")

        # Quick verification
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM public.articles")
            result = cur.fetchone()
            print(f"  Verified count in database: {result['count']}")

    except Exception as e:
        print(f"\n✗ Data generation failed: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()

    print("\n✓ Phase 2 data generation completed successfully")


if __name__ == "__main__":
    main()