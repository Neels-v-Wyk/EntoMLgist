# EntoMLgist

Insect detection/classification using YOLO, Visual Transformers, and NLP, using Dagster for continous data ingestion and training on data from Reddit's /r/whatisthisbug.

## Quick Setup

### Prereqs
- Python
- Docker
- uv

### Quick Makefil reference

```bash
# Install dependencies
make deps

# Start PostgreSQL database
make db-up

# Run Dagster dev server
make dagster-dev
```

The Dagster UI will be available at http://localhost:3000

### Running the Pipeline

```bash
# Execute full Reddit scraping pipeline
make dagster-full-reddit-pipeline
```

## Database

The project uses PostgreSQL running in Docker. Data persists in `./postgres_data/`.

Available commands:
- `make db-up` - Start database
- `make db-down` - Stop database
- `make db-shell` - Open psql shell
- `make db-reset` - Delete all data and reset

Database credentials are in `.env` (copy from `.env.example` if needed).

## Architecture

Assets:
- `create_database_tables` - Initialize database schema
- `save_hot_posts_to_db` - Fetch hot posts from /r/whatisthisbug
- `fetch_post_data` - Cache Reddit API responses
- `populate_post_upvotes` - Update post scores
- `populate_comments` - Extract top comments
- `get_image_uris_from_posts` - Extract image URLs
- `download_filtered_pictures` - Download images from high-quality posts

## Project Goals

- [x] Set up dockerized Dagster, every step below is a DAG 
- [x] Retrieve images from reddit's "what is this bug" subreddit
- [x] Retrieve comments from the above as well
- [x] Store already gotten comments and post IDs in a DB so we don't get duplicates in the future
- [ ] Create a sample training data set which is manually labeled for insect identification (location only)
- [ ] Train YOLO to find insects
- [ ] Use NLP to extract insect names from top comments on posts, for use in classification training
- [ ] Set up a ViT to classify insects based on cropped images from YOLO and text data
- [ ] Use playwright to leave comments on posts from ViT output
- [ ] Continously improve the model on new posts

## Why

Too many "is this a bedbug?" posts on /r/whatisthisbug when it's obviously a bedbug or german cockroach. If this works well enough, it can become a bot to answer these repetitive questions automatically.
