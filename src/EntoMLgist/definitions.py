import dagster as dg
from sqlmodel import Session
from gliner2 import GLiNER2
from EntoMLgist.database_config import engine
from EntoMLgist.defs.assets.reddit.database import create_database_tables
from EntoMLgist.defs.assets.reddit.posts import save_hot_posts_to_db
from EntoMLgist.defs.assets.reddit.data_population import (
    fetch_post_data,
    populate_post_upvotes,
    populate_comments,
)
from EntoMLgist.defs.assets.reddit.download import (
    get_image_uris_from_posts,
    # download_all_pictures,  # Temporarily disabled
    download_filtered_pictures,
)
from EntoMLgist.defs.assets.nlp.comment_extraction import extract_insect_names_from_comments
from EntoMLgist.defs.assets.nlp.name_normalization import normalize_insect_names
from EntoMLgist.defs.jobs import all_assets_job, full_reddit_pipeline_job

@dg.resource
def db_session_resource(context):
    """SQLModel database session resource."""
    with Session(engine) as session:
        context.log.info("Database session created")
        try:
            yield session
        except Exception as e:
            session.rollback()
            context.log.error(f"Database session error: {e}")
            raise
        finally:
            context.log.info("Database session closed")

@dg.resource
def gliner_extractor_resource(context):
    """GLiNER2 model resource - loads once and persists across assets."""
    context.log.info("Loading GLiNER2 model...")
    extractor = GLiNER2.from_pretrained("fastino/gliner2-base-v1")
    # TODO: Add parameterization for different models or custom models
    context.log.info("GLiNER2 model loaded successfully")
    return extractor

all_assets = [
    create_database_tables,
    save_hot_posts_to_db,
    fetch_post_data,
    populate_post_upvotes,
    populate_comments,
    get_image_uris_from_posts,
    # download_all_pictures,  # Temporarily disabled
    download_filtered_pictures,
    extract_insect_names_from_comments,
    normalize_insect_names,
]

defs = dg.Definitions(
    assets=all_assets,
    jobs=[full_reddit_pipeline_job, all_assets_job],
    resources={
        "db_session": db_session_resource,
        "gliner_extractor": gliner_extractor_resource,
    },
)
