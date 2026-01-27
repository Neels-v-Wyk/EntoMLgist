"""SQLModel database models for Reddit data storage."""
from typing import Optional
from sqlmodel import Field, SQLModel, Relationship


class Post(SQLModel, table=True):
    """Reddit post model."""
    __tablename__ = "posts"
    
    post_id: str = Field(primary_key=True, description="Reddit post ID")
    title: str = Field(description="Post title")
    upvotes: int = Field(default=0, description="Number of upvotes")
    extracted_location: Optional[str] = Field(default=None, description="Extracted insect location from post, if any")
    extracted_location_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    
    # Relationships
    comments: list["Comment"] = Relationship(back_populates="post", cascade_delete=True)
    image_urls: list["ImageUrl"] = Relationship(back_populates="post", cascade_delete=True)


class Comment(SQLModel, table=True):
    """Reddit comment model."""
    __tablename__ = "comments"
    
    comment_id: str = Field(primary_key=True, description="Reddit comment ID")
    parent_post_id: str = Field(foreign_key="posts.post_id", description="Parent post ID")
    body: str = Field(description="Comment text content")
    upvotes: int = Field(default=0, description="Number of upvotes")
    extracted_name: Optional[str] = Field(default=None, description="Extracted insect name from comment, if any")
    extracted_name_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    extracted_species: Optional[str] = Field(default=None, description="Extracted insect species from comment, if any")
    extracted_species_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    extracted_genus: Optional[str] = Field(default=None, description="Extracted insect genus from comment, if any")
    extracted_genus_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    
    # Relationships
    post: Optional[Post] = Relationship(back_populates="comments")


class ImageUrl(SQLModel, table=True):
    """Image URL tracking model."""
    __tablename__ = "image_urls"
    
    image_id: str = Field(primary_key=True, description="Unique image identifier")
    parent_post_id: str = Field(foreign_key="posts.post_id", description="Parent post ID")
    url: str = Field(description="Image URL")
    extension: Optional[str] = Field(default=None, description="File extension")
    local_path: Optional[str] = Field(default=None, description="Local file path after download")
    downloaded: int = Field(default=0, description="Download status (0=not downloaded, 1=downloaded)")
    
    # Relationships
    post: Optional[Post] = Relationship(back_populates="image_urls")
