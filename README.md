# EntoMLgist

An automated insect identification system for Reddit's /r/whatisthisbug using semi-automatic labeling, taxonomy normalization, and custom ML models. Dagster orchestrates continuous data ingestion, comment analysis, and model training.

## Quick Setup

### Prereqs
- Python
- Docker
- uv

### Quick Makefile reference

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

### Data Ingestion Pipeline (WIP)
- `create_database_tables` - Initialize database schema
- `save_hot_posts_to_db` - Fetch hot posts from /r/whatisthisbug
- `fetch_post_data` - Cache Reddit API responses
- `populate_post_upvotes` - Update post scores
- `populate_comments` - Extract top comments
- `get_image_uris_from_posts` - Extract image URLs
- `download_filtered_pictures` - Download images from high-quality posts

### Labeling & Taxonomy Pipeline (UNIMPLEMENTED)
- `extract_insect_names_from_comments` - Extract insect mentions from Reddit comments
- `normalize_insect_names` - Query GBIF API to map common names → scientific taxonomy
- `consensus_labeling` - Filter posts by confidence threshold (multiple sources agreeing)
- `generate_training_dataset` - Prepare clean, labeled dataset

### Model Training & Inference Pipeline (UNIMPLEMENTED)
- `generate_weak_yolo_labels` - Use weak YOLO to auto-generate bounding boxes
- `validate_annotations` - Flag auto-generated boxes for human review
- `train_detection_model` - Train YOLOv11 on annotated dataset
- `fine_tune_vit_classifier` - Fine-tune Vision Transformer on cropped insect images
- `infer_on_new_posts` - Run YOLO detection + ViT classification on new Reddit posts
- `format_bot_response` - Generate Reddit comment responses

## Implementation

### Semi-Automatic Labeling (WIP)
- [x] Set up dockerized Dagster for ETL
- [x] Retrieve images and comments from /r/whatisthisbug
- [x] Store data in PostgreSQL (no duplicates)
- [ ] Extract insect names from comments (semantic NLP parsing)
- [ ] Normalize names to scientific taxonomy via GBIF API (handle "ladybug"/"ladybird" synonyms)
- [ ] Build consensus scoring (filter posts where multiple comments agree on ID)
- [ ] Collect 500-1000 high-confidence labeled images

### Object Detection
- [ ] Manual annotation of 150-200 images with bounding boxes (Streamlit UI or CVAT)
- [ ] Export annotations in YOLO format
- [ ] Train YOLOv11 detection model on weak annotated set
- [ ] Evaluate detection performance on holdout set

### Classification (ViT)
- [ ] Use trained YOLO to auto-generate bounding boxes on full 500-1000 labeled dataset
- [ ] Active learning: flag low-confidence boxes for human review and correction
- [ ] Fine-tune Vision Transformer (ViT) on cropped insect images
- [ ] Validate classification accuracy (~70%+ target)

### Deployment
- [ ] Integrate YOLO + classifier into Dagster pipeline
- [ ] Build Reddit bot poster using playwright
- [ ] Test bot responses on recent posts
- [ ] Monitor bot accuracy and collect feedback
- [ ] Set up continuous retraining on new posts (weekly)

### Scaling / Improvement
- [ ] Improve classification on edge cases (multiple insects, unclear images)
- [ ] Fine-tune models on user feedback and misclassifications
- [ ] Handle multi-insect images (post-process YOLO detections)
- [ ] Performance optimization (model quantization, inference speed)

## Why

I got tired of too many "is this a bedbug?" posts on /r/whatisthisbug when it's obviously a bedbug or german cockroach. If this works well enough, it can become a bot to answer these repetitive questions automatically.
