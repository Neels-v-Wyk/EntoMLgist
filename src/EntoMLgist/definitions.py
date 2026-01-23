import dagster as dg
from sqlmodel import Session
from EntoMLgist.database_config import engine
from EntoMLgist.defs.assets.pictures.database import create_database_tables
from EntoMLgist.defs.assets.pictures.posts import save_hot_posts_to_db
from EntoMLgist.defs.assets.pictures.data_population import (
    fetch_post_data,
    populate_post_upvotes,
    populate_comments,
)
from EntoMLgist.defs.assets.pictures.download import (
    get_image_uris_from_posts,
    download_all_pictures,
    download_filtered_pictures,
)
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

all_assets = [
    create_database_tables,
    save_hot_posts_to_db,
    fetch_post_data,
    populate_post_upvotes,
    populate_comments,
    get_image_uris_from_posts,
    download_all_pictures,
    download_filtered_pictures,
]

defs = dg.Definitions(
    assets=all_assets,
    jobs=[full_reddit_pipeline_job, all_assets_job],
    resources={
        "db_session": db_session_resource,
    },
)
