import dagster as dg
from EntoMLgist.defs.assets.reddit.database import create_database_tables
from EntoMLgist.defs.assets.reddit.posts import save_hot_posts_to_db
from EntoMLgist.defs.assets.reddit.data_population import (
    fetch_post_data,
    populate_post_upvotes,
    populate_comments,
)
from EntoMLgist.defs.assets.reddit.download import (
    get_image_uris_from_posts,
    download_filtered_pictures,
)
from EntoMLgist.defs.assets.nlp.comment_extraction import extract_insect_names_from_comments
from EntoMLgist.defs.assets.nlp.name_normalization import normalize_insect_names
from EntoMLgist.defs.assets.nlp.gbif_enrichment import (
    enrich_taxonomy_from_gbif,
    link_images_to_taxonomy,
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
        download_filtered_pictures,
        extract_insect_names_from_comments,
        normalize_insect_names,
        enrich_taxonomy_from_gbif,
        link_images_to_taxonomy,
    )
)
