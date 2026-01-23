import dagster as dg
from sqlmodel import SQLModel, Session, text
from EntoMLgist.models.database import Post, Comment, ImageUrl
from EntoMLgist.database_config import engine


@dg.asset(required_resource_keys={"db_session"})
def create_database_tables(context: dg.AssetExecutionContext):
    """Create all database tables using SQLModel.
    
    SQLModel automatically creates tables with proper foreign keys,
    indexes, and constraints based on the model definitions.
    """
    try:
        # Create all tables
        SQLModel.metadata.create_all(engine)
        context.log.info("Database tables created successfully")
        
        # Create additional indexes for better query performance
        session: Session = context.resources.db_session
        
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
        
        session.commit()
        context.log.info("Database indexes created successfully")
        
    except Exception as e:
        context.log.error(f"Error creating database tables: {e}")
        raise

