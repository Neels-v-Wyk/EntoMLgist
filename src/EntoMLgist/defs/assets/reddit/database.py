import dagster as dg
from sqlmodel import SQLModel, Session, text
from EntoMLgist.models.database import Post, Comment, ImageUrl, TaxonomicName, ImageTaxonomyLink
from EntoMLgist.database_config import engine


@dg.asset(required_resource_keys={"db_session"})
def create_database_tables(context: dg.AssetExecutionContext):
    """Create all database tables using SQLModel.
    
    SQLModel automatically creates tables with proper foreign keys,
    indexes, and constraints based on the model definitions.
    """
    # TODO: Use migrations framework (or something similar) for schema versioning
    try:
        # Create all tables
        SQLModel.metadata.create_all(engine)
        context.log.info("Database tables created successfully")
        
        session: Session = context.resources.db_session
        
        # Ensure all expected columns exist (handles schema evolution)
        # Add missing columns to comments table
        session.exec(text(
            "ALTER TABLE comments ADD COLUMN IF NOT EXISTS extracted_name TEXT"
        ))
        session.exec(text(
            "ALTER TABLE comments ADD COLUMN IF NOT EXISTS extracted_name_confidence REAL"
        ))
        context.log.info("Ensured comments table has all required columns")
        
        # Create additional indexes for better query performance
        # Index for comments by parent post
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_comments_parent_post ON comments(parent_post_id)"
        ))
        
        # Index for posts by upvotes
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_posts_upvotes ON posts(upvotes DESC)"
        ))
        
        # Index for image_urls by parent post
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_image_urls_parent_post ON image_urls(parent_post_id)"
        ))
        
        # Index for image_urls by downloaded status
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_image_urls_downloaded ON image_urls(downloaded)"
        ))
        
        # Index for comments with extracted names
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_comments_extracted_name ON comments(extracted_name)"
        ))
        
        # Index for taxonomic_names by lookup success
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_taxonomic_names_success ON taxonomic_names(lookup_success)"
        ))
        
        # Index for taxonomic_names by is_insect
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_taxonomic_names_insect ON taxonomic_names(is_insect)"
        ))
        
        # Index for image_taxonomy_links by image_id
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_image_taxonomy_image ON image_taxonomy_links(image_id)"
        ))
        
        # Index for image_taxonomy_links by common_name
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_image_taxonomy_name ON image_taxonomy_links(common_name)"
        ))
        
        # Add new columns to image_taxonomy_links for quality scoring
        session.exec(text(
            "ALTER TABLE image_taxonomy_links ADD COLUMN IF NOT EXISTS gbif_confidence INTEGER"
        ))
        session.exec(text(
            "ALTER TABLE image_taxonomy_links ADD COLUMN IF NOT EXISTS label_quality_score REAL"
        ))
        
        # Index for filtering by label quality (critical for training data selection)
        session.exec(text(
            "CREATE INDEX IF NOT EXISTS idx_image_taxonomy_quality ON image_taxonomy_links(label_quality_score DESC)"
        ))
        context.log.info("Ensured image_taxonomy_links has quality score columns")
        
        session.commit()
        context.log.info("Database indexes created successfully")
        
    except Exception as e:
        context.log.error(f"Error creating database tables: {e}")
        raise

