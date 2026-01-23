import dagster as dg
from EntoMLgist.defs.assets.pictures.database import create_database_tables
from EntoMLgist.defs.assets.pictures.posts import save_hot_posts_to_db
from EntoMLgist.defs.assets.pictures.data_population import (
    fetch_post_data,
    populate_post_upvotes,
    populate_comments,
)
from EntoMLgist.defs.assets.pictures.download import (
    get_image_uris_from_posts,
    download_filtered_pictures,
)

all_assets_job = dg.define_asset_job(name="all_assets_job")

full_reddit_pipeline_job = dg.define_asset_job(
    name="full_reddit_pipeline_job",
    selection=dg.AssetSelection.assets(
        create_database_tables,
        save_hot_posts_to_db,
        fetch_post_data,
        populate_post_upvotes,
        populate_comments,
        get_image_uris_from_posts,
        download_filtered_pictures
    )
)
