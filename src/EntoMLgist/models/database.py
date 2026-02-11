"""SQLModel database models for Reddit data storage."""
from typing import Optional
from sqlmodel import Field, SQLModel, Relationship


class Post(SQLModel, table=True):
    """Reddit post model."""
    __tablename__ = "posts"
    
    post_id: str = Field(primary_key=True, description="Reddit post ID")
    title: str = Field(description="Post title")
    upvotes: int = Field(default=0, description="Number of upvotes")
    
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


class TaxonomicName(SQLModel, table=True):
    """Taxonomic information for normalized insect common names."""
    __tablename__ = "taxonomic_names"
    
    # Primary key is the normalized common name
    common_name: str = Field(primary_key=True, description="Normalized common name from comments")
    
    # GBIF identifiers
    gbif_usage_key: Optional[int] = Field(default=None, description="GBIF taxon usage key")
    gbif_accepted_key: Optional[int] = Field(default=None, description="GBIF accepted taxon key")
    gbif_taxonomic_status: Optional[str] = Field(default=None, description="Taxonomic status (ACCEPTED, SYNONYM, etc.)")
    gbif_match_type: Optional[str] = Field(default=None, description="GBIF match type (EXACT, FUZZY, HIGHERRANK)")
    gbif_confidence: Optional[int] = Field(default=None, ge=0, le=100, description="GBIF match confidence score")
    
    # Scientific nomenclature
    scientific_name: Optional[str] = Field(default=None, description="Scientific binomial name")
    canonical_name: Optional[str] = Field(default=None, description="Canonical form without authorship")
    
    # Taxonomic hierarchy
    kingdom: Optional[str] = Field(default=None, description="Kingdom (typically Animalia)")
    phylum: Optional[str] = Field(default=None, description="Phylum (e.g., Arthropoda)")
    class_name: Optional[str] = Field(default=None, description="Class (e.g., Insecta)")
    order: Optional[str] = Field(default=None, description="Order (e.g., Coleoptera, Lepidoptera)")
    family: Optional[str] = Field(default=None, description="Family (e.g., Coccinellidae)")
    genus: Optional[str] = Field(default=None, description="Genus (e.g., Coccinella)")
    species: Optional[str] = Field(default=None, description="Species epithet")
    
    # Taxonomic rank
    rank: Optional[str] = Field(default=None, description="Taxonomic rank (SPECIES, GENUS, FAMILY, etc.)")
    
    # Additional common names from GBIF
    vernacular_names: Optional[str] = Field(default=None, description="JSON array of alternative common names")
    
    # Metadata
    lookup_timestamp: Optional[int] = Field(default=None, description="Unix timestamp of GBIF lookup")
    lookup_success: bool = Field(default=False, description="Whether GBIF lookup was successful")
    lookup_error: Optional[str] = Field(default=None, description="Error message if lookup failed")
    
    # Quality indicators
    is_insect: Optional[bool] = Field(default=None, description="Whether the taxon is confirmed to be an insect")
    num_occurrences: Optional[int] = Field(default=None, description="Number of occurrences in GBIF database")


class ImageTaxonomyLink(SQLModel, table=True):
    """Links images to their taxonomic classifications via comments."""
    __tablename__ = "image_taxonomy_links"
    
    # Composite primary key
    image_id: str = Field(foreign_key="image_urls.image_id", primary_key=True, description="Image identifier")
    common_name: str = Field(foreign_key="taxonomic_names.common_name", primary_key=True, description="Associated taxonomic common name")
    
    # Link metadata
    comment_id: str = Field(foreign_key="comments.comment_id", description="Source comment for this classification")
    extraction_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="GLiNER extraction confidence")
    gbif_confidence: Optional[int] = Field(default=None, ge=0, le=100, description="GBIF match confidence score")
    
    # Combined quality score for training data filtering
    label_quality_score: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="Combined confidence score (extraction * GBIF/100)")
    
    # Voting/quality indicators (for future use with multiple identifications)
    comment_upvotes: int = Field(default=0, description="Upvotes on the source comment")
    is_top_identification: bool = Field(default=False, description="Whether this is the highest-voted identification")
