# Code Improvements and Best Practices Guide

## Overview
This document outlines architectural, performance, and maintainability improvements identified in the EntoMLgist picture-fetching pipeline. Each section corresponds to specific TODOs added to the codebase.

---

## 1. Configuration Management

### Issue: Magic Numbers and Hard-coded Values
**File:** `constants.py`

**Current State:** Configuration values are scattered as module-level constants without validation.

**Problem:**
- No separation between development, staging, and production environments
- Difficult to manage different settings for different deployment contexts
- Type safety is not enforced
- No centralized documentation of valid ranges

**Recommendation:**
Implement a configuration management system using one of these approaches:

**Option A: Environment Variables (Recommended for Docker/Cloud)**
```python
# constants.py - Enhanced
import os
from pathlib import Path
from typing import Annotated

from pydantic import BaseSettings, Field, validator

class Settings(BaseSettings):
    # API Configuration
    subreddit: str = Field(default="whatisthisbug", description="Target subreddit")
    user_agent: str = Field(default="Mozilla/5.0 ...", description="User agent for requests")
    
    # Thresholds
    image_download_upvote_threshold: int = Field(default=5, ge=0, description="Minimum upvotes for download")
    image_comment_count_threshold: int = Field(default=3, ge=0, description="Minimum comments for training data")
    post_top_comments_num: int = Field(default=3, ge=1, description="Number of top comments to fetch")
    
    # Performance
    post_crawl_delay: float = Field(default=1.0, gt=0, description="Delay between post iterations (seconds)")
    max_backoff: int = Field(default=60, gt=0, description="Maximum backoff for rate limiting (seconds)")
    total_pictures: int = Field(default=100, ge=1, description="Total pictures to fetch per run")
    
    # Paths
    image_download_path: Path = Field(default="./downloads/images", description="Local image storage path")
    
    class Config:
        env_file = ".env"
        env_prefix = "ENTOM_"
    
    @validator("image_download_path", pre=True)
    def ensure_path_exists(cls, v):
        path = Path(v)
        path.mkdir(parents=True, exist_ok=True)
        return path

settings = Settings()
```

**Option B: YAML Configuration (Recommended for local development)**
```yaml
# config.yaml
api:
  subreddit: whatisthisbug
  user_agent: Mozilla/5.0 ...
  post_crawl_delay: 1

thresholds:
  image_download_upvotes: 5
  comment_count: 3
  top_comments: 3

paths:
  downloads: ./downloads/images

performance:
  max_backoff: 60
  total_pictures: 100
```

**Benefits:**
- Environment-specific configuration
- Type validation at runtime
- Clear documentation of valid ranges
- Easy to override for testing

---

## 2. API Abstraction and Reusability

### Issue: Repeated Request Logic
**File:** `data_population.py`

**Problem:**
- URL building, headers, and retry logic scattered across multiple functions
- Difficult to test individual components
- Hard to maintain consistency across API calls

**Recommendation:**

Create a Reddit API client class:

```python
# reddit_client.py - NEW FILE
from typing import Optional
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from time import sleep
from EntoMLgist.defs.assets.pictures.constants import (
    SUBREDDIT, DEFAULT_USER_AGENT, MAX_BACKOFF
)

@dataclass
class RedditConfig:
    subreddit: str
    user_agent: str
    base_url: str = "https://www.reddit.com"
    timeout: int = 10

class RedditAPIClient:
    """Encapsulates Reddit API interactions with resilience features."""
    
    def __init__(self, config: RedditConfig):
        self.config = config
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """Create a session with connection pooling and retry strategy."""
        session = requests.Session()
        
        # Connection pooling
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504]
            )
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        
        return session
    
    def get_post_comments(self, post_id: str) -> Optional[dict]:
        """Fetch post data with exponential backoff for rate limiting."""
        url = f"{self.config.base_url}/r/{self.config.subreddit}/comments/{post_id}.json"
        headers = {'User-Agent': self.config.user_agent}
        
        try:
            response = self.session.get(url, headers=headers, timeout=self.config.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            # Detailed error handling
            raise
    
    def get_hot_posts(self, limit: int) -> Optional[dict]:
        """Fetch hot posts from subreddit."""
        url = f"{self.config.base_url}/r/{self.config.subreddit}/hot.json"
        params = {'limit': limit}
        headers = {'User-Agent': self.config.user_agent}
        
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=self.config.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise
```

**Usage:**
```python
# In data_population.py
client = RedditAPIClient(RedditConfig(subreddit=SUBREDDIT, user_agent=DEFAULT_USER_AGENT))
response_data = client.get_post_comments(post_id)
```

**Benefits:**
- Single source of truth for API logic
- Connection pooling improves performance
- Automatic retry handling
- Easy to mock in tests
- Centralized configuration

---

## 3. Defensive Parsing and Logging

### Issue: Silent Failures and Brittle JSON Parsing
**File:** `data_population.py` (`extract_comments`, `populate_post_upvotes`)

**Problem:**
- Using bare `except KeyError` with silent prints instead of structured logging
- No sensible defaults for missing data
- Hard to debug parsing failures in production

**Recommendation:**

Create a JSON parsing helper module:

```python
# json_parser.py - NEW FILE
import logging
from typing import Any, Dict, Optional, Callable
from functools import wraps

logger = logging.getLogger(__name__)

class JSONParseError(Exception):
    """Custom exception for JSON parsing failures."""
    pass

class SafeJSONParser:
    """Safely extracts nested values from JSON with logging and defaults."""
    
    @staticmethod
    def get_nested(
        data: dict,
        keys: list[str],
        default: Any = None,
        log_warning: bool = True
    ) -> Any:
        """
        Safely get nested value from dict.
        
        Args:
            data: The dictionary to parse
            keys: List of keys forming the path (e.g., ['data', 'children', 0, 'data'])
            default: Value to return if path doesn't exist
            log_warning: Whether to log a warning if key not found
        
        Returns:
            The value at the path or default
        """
        current = data
        path_str = " -> ".join(str(k) for k in keys)
        
        try:
            for key in keys:
                if isinstance(current, dict):
                    current = current[key]
                elif isinstance(current, list):
                    current = current[int(key)]
                else:
                    raise TypeError(f"Cannot index {type(current).__name__}")
            return current
        except (KeyError, IndexError, ValueError, TypeError) as e:
            if log_warning:
                logger.warning(f"Failed to parse path '{path_str}': {e}. Using default: {default}")
            return default

def log_parse_errors(func: Callable) -> Callable:
    """Decorator to log parsing errors with context."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except JSONParseError as e:
            logger.error(f"Parse error in {func.__name__}: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper
```

**Updated Usage:**
```python
# In data_population.py
from EntoMLgist.utils.json_parser import SafeJSONParser

def populate_post_upvotes(context: dg.AssetExecutionContext, fetch_post_data: dict):
    session: Session = context.resources.db_session
    posts = get_posts_from_db(session)

    for post in posts:
        if post.post_id not in fetch_post_data:
            context.log.warning(f"No cached data for post {post.post_id}")
            continue
        
        data = fetch_post_data[post.post_id]
        
        # Safe parsing with defaults
        upvotes = SafeJSONParser.get_nested(
            data,
            [0, 'data', 'children', 0, 'data', 'ups'],
            default=0,
            log_warning=True
        )
        
        post.upvotes = upvotes
        context.log.info(f"Post {post.post_id} has {upvotes} upvotes")
        session.add(post)
    
    session.commit()
```

**Benefits:**
- Structured error logging for debugging
- Transparent defaults make data quality visible
- Easier to identify problematic posts
- More resilient to API changes

---

## 4. Configurable Filters and Extensibility

### Issue: Hard-coded Filter Logic
**File:** `data_population.py` (`extract_comments`)

**Problem:**
- AutoModerator filter is hard-coded
- Cannot easily add new filters (e.g., by score, date)
- Not extensible for different use cases

**Recommendation:**

```python
# filters.py - NEW FILE
from dataclasses import dataclass
from typing import Callable, List
from EntoMLgist.models.reddit import RedditComment

@dataclass
class CommentFilter:
    """Base class for comment filtering logic."""
    name: str
    predicate: Callable[[dict], bool]

class CommentFilterRegistry:
    """Registry for composable comment filters."""
    
    def __init__(self):
        self.filters: List[CommentFilter] = []
    
    @staticmethod
    def exclude_automod(comment_data: dict) -> bool:
        """Exclude AutoModerator comments."""
        return comment_data.get('author', '') != 'AutoModerator'
    
    @staticmethod
    def min_score(min_upvotes: int) -> Callable[[dict], bool]:
        """Exclude comments below score threshold."""
        return lambda c: c.get('ups', 0) >= min_upvotes
    
    @staticmethod
    def exclude_deleted(comment_data: dict) -> bool:
        """Exclude deleted or removed comments."""
        return comment_data.get('body', '').strip() not in ['[deleted]', '[removed]']
    
    def add_filter(self, filter_obj: CommentFilter) -> 'CommentFilterRegistry':
        """Chain filters fluently."""
        self.filters.append(filter_obj)
        return self
    
    def apply(self, comment_data: dict) -> bool:
        """Apply all filters (all must pass)."""
        return all(f.predicate(comment_data) for f in self.filters)

# Usage:
filters = CommentFilterRegistry()
filters.add_filter(CommentFilter("exclude_automod", CommentFilterRegistry.exclude_automod))
filters.add_filter(CommentFilter("min_score", CommentFilterRegistry.min_score(1)))
filters.add_filter(CommentFilter("exclude_deleted", CommentFilterRegistry.exclude_deleted))

def extract_comments(comments_data, post_id, filter_registry: CommentFilterRegistry = None):
    if filter_registry is None:
        filter_registry = CommentFilterRegistry()  # Default filters
    
    comments = []
    for comment in comments_data:
        if comment['kind'] != 't1':
            continue
        
        comment_data = comment['data']
        
        if not filter_registry.apply(comment_data):
            continue
        
        try:
            reddit_comment = RedditComment(
                parent_post_id=post_id,
                comment_id=comment_data.get('id'),
                body=comment_data.get('body', ''),
                upvotes=comment_data.get('ups', 0)
            )
            comments.append(reddit_comment)
        except KeyError as e:
            logger.warning(f"Skipping invalid comment: {e}")
    
    return comments
```

**Benefits:**
- Filters are composable and reusable
- Easy to add/remove filters without changing core logic
- Testable in isolation
- Self-documenting code

---

## 5. Persistent Caching Strategy

### Issue: In-Memory Cache Loss
**File:** `data_population.py` (`fetch_post_data`)

**Problem:**
- Cache is lost if Dagster re-runs the DAG
- Expensive API calls repeated unnecessarily
- No cache invalidation strategy

**Recommendation:**

Implement a multi-tier caching strategy:

```python
# cache_manager.py - NEW FILE
import pickle
import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, TypeVar, Generic
import hashlib
import logging

logger = logging.getLogger(__name__)
T = TypeVar('T')

class CacheManager(Generic[T]):
    """Multi-tier cache: memory → disk → source."""
    
    def __init__(self, cache_dir: Path = Path("./cache"), ttl_hours: int = 24):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl = timedelta(hours=ttl_hours)
        self._memory_cache = {}
    
    def _get_cache_key(self, key: str) -> str:
        """Generate cache filename from key."""
        return hashlib.sha256(key.encode()).hexdigest()
    
    def get(self, key: str, fetch_fn: Callable[[], T]) -> T:
        """
        Get value from cache or fetch from source.
        
        Checks in order: memory → disk → source function
        """
        # Check memory cache
        if key in self._memory_cache:
            logger.debug(f"Cache hit (memory): {key}")
            return self._memory_cache[key]
        
        # Check disk cache
        cache_file = self.cache_dir / self._get_cache_key(key)
        if cache_file.exists():
            try:
                mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if datetime.now() - mtime < self.ttl:
                    logger.debug(f"Cache hit (disk): {key}")
                    with open(cache_file, 'rb') as f:
                        data = pickle.load(f)
                    self._memory_cache[key] = data
                    return data
                else:
                    logger.debug(f"Cache expired: {key}")
                    cache_file.unlink()
            except Exception as e:
                logger.warning(f"Failed to read cache: {e}")
        
        # Fetch from source
        logger.info(f"Cache miss, fetching: {key}")
        data = fetch_fn()
        self.set(key, data)
        return data
    
    def set(self, key: str, value: T) -> None:
        """Store value in memory and disk cache."""
        self._memory_cache[key] = value
        
        cache_file = self.cache_dir / self._get_cache_key(key)
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(value, f)
            logger.debug(f"Cached: {key}")
        except Exception as e:
            logger.error(f"Failed to write cache: {e}")
    
    def clear(self, older_than_hours: int = None) -> None:
        """Clear cache, optionally by age."""
        self._memory_cache.clear()
        
        if older_than_hours:
            cutoff = datetime.now() - timedelta(hours=older_than_hours)
            for cache_file in self.cache_dir.glob('*'):
                if datetime.fromtimestamp(cache_file.stat().st_mtime) < cutoff:
                    cache_file.unlink()
        else:
            for cache_file in self.cache_dir.glob('*'):
                cache_file.unlink()

# Usage in DAG:
cache = CacheManager(ttl_hours=24)

@dg.asset(required_resource_keys={"db_session"}, deps=["save_hot_posts_to_db"])
def fetch_post_data(context: dg.AssetExecutionContext) -> dict:
    session: Session = context.resources.db_session
    posts = get_posts_from_db(session)
    
    post_data_cache = {}
    for post in posts:
        context.log.info(f"Fetching post data for {post.post_id}")
        
        def fetch():
            response = retrieve_post_data(post.post_id)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch {post.post_id}")
            sleep(POST_CRAWL_DELAY)
            return response.json()
        
        try:
            post_data_cache[post.post_id] = cache.get(f"post_{post.post_id}", fetch)
        except Exception as e:
            context.log.error(f"Error fetching data for post {post.post_id}: {e}")
    
    return post_data_cache
```

**Benefits:**
- Reduced API calls (cost & bandwidth)
- Faster DAG execution on re-runs
- Clear cache invalidation strategy
- Transparent to rest of code

---

## 6. Database Migrations (Version Control)

### Issue: No Schema Version Management
**File:** `database.py`

**Problem:**
- Schema changes are difficult to track
- No rollback capability
- Hard to deploy to different environments

**Recommendation:**

Use Alembic for database migrations:

```bash
# Installation
pip install alembic

# Initialization (run once)
alembic init alembic

# Create migration
alembic revision --autogenerate -m "Add post and comment tables"

# Apply migrations
alembic upgrade head

# Rollback
alembic downgrade -1
```

**Example migration:**
```python
# alembic/versions/001_initial_schema.py
from alembic import op
import sqlalchemy as sa

def upgrade():
    op.create_table(
        'posts',
        sa.Column('post_id', sa.String(), nullable=False),
        sa.Column('title', sa.String(), nullable=False),
        sa.Column('upvotes', sa.Integer(), server_default='0'),
        sa.PrimaryKeyConstraint('post_id')
    )
    op.create_index('idx_posts_upvotes', 'posts', ['upvotes'], unique=False)

def downgrade():
    op.drop_table('posts')
```

**Updated create_database_tables:**
```python
@dg.asset(required_resource_keys={"db_session"})
def create_database_tables(context: dg.AssetExecutionContext):
    """Apply all pending database migrations using Alembic."""
    from alembic.config import Config
    from alembic.command import upgrade
    
    try:
        alembic_cfg = Config("alembic.ini")
        upgrade(alembic_cfg, "head")
        context.log.info("Database migrations applied successfully")
    except Exception as e:
        context.log.error(f"Error applying migrations: {e}")
        raise
```

**Benefits:**
- Version-controlled schema changes
- Easy deployments to multiple environments
- Rollback capability
- Team collaboration on schema changes

---

## 7. Image Download Resilience

### Issue: No Retry Logic or Validation
**File:** `download.py` (`download_image_from_uri`)

**Problem:**
- Single attempt to download (no retries)
- No verification of downloaded file integrity
- Large files not handled efficiently
- No progress tracking

**Recommendation:**

```python
# image_downloader.py - NEW FILE
import hashlib
import logging
from pathlib import Path
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from PIL import Image
from io import BytesIO

logger = logging.getLogger(__name__)

class ImageDownloader:
    """Robust image downloading with retries, validation, and streaming."""
    
    VALID_EXTENSIONS = {'jpg', 'jpeg', 'png', 'gif', 'webp'}
    CHUNK_SIZE = 8192
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
    
    def __init__(self, timeout: int = 10, max_retries: int = 3):
        self.timeout = timeout
        self.session = self._create_session(max_retries)
    
    def _create_session(self, max_retries: int) -> requests.Session:
        """Create session with exponential backoff."""
        session = requests.Session()
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=max_retries,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504]
            )
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        return session
    
    def validate_image(self, file_path: Path) -> bool:
        """Validate image file integrity using PIL."""
        try:
            img = Image.open(file_path)
            img.verify()
            return True
        except Exception as e:
            logger.error(f"Image validation failed for {file_path}: {e}")
            return False
    
    def calculate_hash(self, file_path: Path) -> str:
        """Calculate SHA256 hash of file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def download(
        self,
        url: str,
        local_path: Path,
        validate: bool = True,
        expected_hash: Optional[str] = None
    ) -> bool:
        """
        Download image with validation.
        
        Args:
            url: Image URL
            local_path: Where to save
            validate: Run PIL validation
            expected_hash: Optional SHA256 to verify
        
        Returns:
            True if successful
        """
        try:
            import html
            url = html.unescape(url)
            
            headers = {
                'User-Agent': DEFAULT_USER_AGENT,
                'Referer': 'https://reddit.com/',
                'Accept': 'image/*',
            }
            
            response = self.session.get(
                url,
                headers=headers,
                timeout=self.timeout,
                stream=True
            )
            response.raise_for_status()
            
            # Check Content-Length before downloading
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) > self.MAX_FILE_SIZE:
                logger.error(f"File too large: {content_length} bytes")
                return False
            
            # Stream download with size check
            local_path.parent.mkdir(parents=True, exist_ok=True)
            downloaded_size = 0
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=self.CHUNK_SIZE):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        if downloaded_size > self.MAX_FILE_SIZE:
                            local_path.unlink()
                            logger.error("Download exceeded max file size")
                            return False
            
            # Validate
            if validate and not self.validate_image(local_path):
                local_path.unlink()
                return False
            
            # Verify hash if provided
            if expected_hash:
                actual_hash = self.calculate_hash(local_path)
                if actual_hash != expected_hash:
                    logger.error(f"Hash mismatch for {local_path}")
                    local_path.unlink()
                    return False
            
            logger.info(f"Downloaded: {local_path}")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed for {url}: {e}")
            if local_path.exists():
                local_path.unlink()
            return False
        except Exception as e:
            logger.error(f"Unexpected error downloading {url}: {e}")
            if local_path.exists():
                local_path.unlink()
            return False

# Usage in download.py:
downloader = ImageDownloader(timeout=10, max_retries=3)

@dg.asset(required_resource_keys={"db_session"})
def download_all_pictures(context: dg.AssetExecutionContext):
    session: Session = context.resources.db_session
    statement = select(ImageUrl).where(ImageUrl.downloaded == 0)
    images = session.exec(statement).all()
    
    for image in images:
        local_path = Path(IMAGE_DOWNLOAD_PATH) / f"{image.parent_post_id}-{image.image_id}.{image.extension}"
        context.log.info(f"Downloading image {image.image_id}")
        
        if downloader.download(image.url, local_path, validate=True):
            image.downloaded = 1
            image.local_path = str(local_path)
            session.add(image)
            session.commit()
        else:
            context.log.error(f"Failed to download {image.image_id}")
```

**Benefits:**
- Automatic retries with exponential backoff
- File integrity validation
- Streaming prevents memory overflow
- Hash verification for data integrity
- Better error messages

---

## 8. Query Optimization (N+1 Problem)

### Issue: Inefficient Database Queries
**File:** `download.py` (`download_filtered_pictures`)

**Problem:**
```python
# Current: N+1 query problem
for image in images:
    comment_count_stmt = select(func.count(Comment.comment_id))...
    comment_count = session.exec(comment_count_stmt).one()  # Extra query per image!
```

**Recommendation:**

```python
# Optimized: Single query with aggregation
@dg.asset(required_resource_keys={"db_session"})
def download_filtered_pictures(context: dg.AssetExecutionContext):
    from sqlmodel import func
    from EntoMLgist.models.database import Comment
    
    session: Session = context.resources.db_session
    
    # Single query with comment count aggregation
    statement = (
        select(
            ImageUrl,
            func.count(Comment.comment_id).label('comment_count')
        )
        .join(Post, ImageUrl.parent_post_id == Post.post_id)
        .outerjoin(Comment, Comment.parent_post_id == Post.post_id)
        .where(
            Post.upvotes >= IMAGE_DOWNLOAD_UPVOTE_THRESHOLD,
            ImageUrl.downloaded == 0
        )
        .group_by(ImageUrl.image_id, Post.post_id)
        .having(func.count(Comment.comment_id) >= IMAGE_COMMENT_COUNT_THRESHOLD)
    )
    
    results = session.exec(statement).all()
    
    for image, comment_count in results:
        local_path = Path(IMAGE_DOWNLOAD_PATH) / f"{image.parent_post_id}-{image.image_id}.{image.extension}"
        
        if downloader.download(image.url, local_path):
            image.downloaded = 1
            image.local_path = str(local_path)
            session.add(image)
    
    session.commit()
```

**Benefits:**
- Single database round-trip instead of N+1
- Dramatic performance improvement (especially with large datasets)
- Reduced database load
- Clearer intent

---

## 9. Type Safety and Data Classes

### Issue: Loose Typing and Dict Magic
**File:** `download.py` (`get_image_uris_from_response`)

**Problem:**
```python
# Current: Difficult to track what's in dicts
image_uris.append({'url': url, 'image_id': media_id, 'extension': extension})
```

**Recommendation:**

```python
# image_models.py - NEW FILE
from dataclasses import dataclass
from typing import Optional

@dataclass
class ImageUri:
    """Represents an image URL extracted from Reddit post."""
    url: str
    image_id: str
    extension: str
    
    def __post_init__(self):
        if not self.url.startswith(('http://', 'https://')):
            raise ValueError(f"Invalid URL: {self.url}")
        if not self.extension.lower() in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
            raise ValueError(f"Invalid extension: {self.extension}")

# Usage:
def get_image_uris_from_response(post_json: dict, post_id: str) -> list[ImageUri]:
    """Extract image URIs from Reddit post JSON response."""
    image_uris: list[ImageUri] = []
    
    try:
        post_data = post_json[0]['data']['children'][0]['data']
        
        if 'media_metadata' in post_data:
            for media_id, media_info in post_data['media_metadata'].items():
                if media_info.get('e') == 'Image' and 's' in media_info:
                    url = media_info['s']['u']
                    extension = get_extension_from_url(url)
                    image_uris.append(ImageUri(url=url, image_id=media_id, extension=extension))
        
        elif 'preview' in post_data and 'images' in post_data['preview']:
            for idx, image in enumerate(post_data['preview']['images']):
                if 'source' in image and 'url' in image['source']:
                    url = image['source']['url']
                    extension = get_extension_from_url(url)
                    if extension.lower() in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                        image_id = generate_image_id(url)
                        image_uris.append(ImageUri(url=url, image_id=image_id, extension=extension))
    
    except Exception as e:
        logger.error(f"Error extracting image URIs from post {post_id}: {e}")
    
    return image_uris
```

**Benefits:**
- Type hints at compile time
- IDE autocomplete and validation
- Self-documenting code
- Runtime validation in `__post_init__`

---

## 10. Bulk Operations for Performance

### Issue: Slow Inserts Due to Individual Commits
**File:** `posts.py` (`save_hot_posts_to_db`)

**Problem:**
```python
# Current: N inserts, N commits (slow)
for post in posts:
    db_post = Post(...)
    session.merge(db_post)

session.commit()  # Only 1 commit, but still slow
```

**Recommendation:**

```python
# Use bulk_insert_mappings for better performance
@dg.asset(required_resource_keys={"db_session"}, deps=["create_database_tables"])
def save_hot_posts_to_db(context: dg.AssetExecutionContext):
    from sqlmodel import Session
    from EntoMLgist.models.database import Post
    
    try:
        posts = get_hot_posts(context)
        
        if not posts:
            context.log.warning("No posts retrieved, skipping database save")
            return
        
        session: Session = context.resources.db_session
        
        # Prepare bulk insert data
        posts_data = [
            {
                'post_id': post.post_id,
                'title': post.title,
                'upvotes': 0
            }
            for post in posts
        ]
        
        # Use upsert for idempotency (UPDATE if exists, INSERT if not)
        from sqlalchemy import insert
        from sqlalchemy.dialects.postgresql import insert as pg_insert
        
        stmt = insert(Post).values(posts_data)
        # For PostgreSQL: use ON CONFLICT to handle duplicates
        stmt = stmt.on_conflict_do_update(
            index_elements=['post_id'],
            set_={'title': stmt.excluded.title}
        )
        
        session.exec(stmt)
        session.commit()
        context.log.info(f"Bulk inserted {len(posts_data)} posts")
        
    except Exception as e:
        context.log.error(f"Error in save_hot_posts_to_db: {e}")
        raise
```

**Benefits:**
- 10-100x faster for large inserts
- Single database round-trip
- Idempotent (safe to re-run)
- Better resource usage

---

## 11. Error Handling and Observability

### General Recommendation Across All Files

**Problem:**
- Inconsistent error handling strategies
- Insufficient context in error messages
- Difficult to debug failures

**Recommendation:**

Create a custom exception hierarchy:

```python
# exceptions.py - NEW FILE
class EntoMLgistException(Exception):
    """Base exception for EntoMLgist."""
    pass

class RedditAPIException(EntoMLgistException):
    """Raised when Reddit API calls fail."""
    def __init__(self, post_id: str, status_code: int, message: str):
        self.post_id = post_id
        self.status_code = status_code
        super().__init__(f"Reddit API error for post {post_id} (HTTP {status_code}): {message}")

class ImageDownloadException(EntoMLgistException):
    """Raised when image download fails."""
    def __init__(self, url: str, reason: str):
        self.url = url
        super().__init__(f"Failed to download image from {url}: {reason}")

class ParseException(EntoMLgistException):
    """Raised when JSON parsing fails."""
    def __init__(self, path: str, expected: type, actual: type):
        super().__init__(f"Parse error at {path}: expected {expected}, got {actual}")
```

**Usage:**
```python
try:
    response = retrieve_post_data(post_id)
    if response.status_code == 429:
        raise RedditAPIException(post_id, 429, "Rate limited")
except RedditAPIException as e:
    context.log.error(e)
    # Send to monitoring/alerting
    notify_team(e)
```

---

## 12. Testing Recommendations

### Add Comprehensive Test Coverage

```python
# tests/test_image_downloader.py
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
from EntoMLgist.defs.assets.pictures.image_downloader import ImageDownloader

class TestImageDownloader:
    
    def test_download_success(self, tmp_path):
        """Test successful image download."""
        downloader = ImageDownloader()
        
        with patch('requests.Session.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {'content-length': '1000'}
            mock_response.iter_content = lambda chunk_size: [b'image_data']
            mock_get.return_value = mock_response
            
            result = downloader.download('https://example.com/image.jpg', tmp_path / 'test.jpg')
            assert result is True
    
    def test_download_file_too_large(self, tmp_path):
        """Test that files exceeding max size are rejected."""
        downloader = ImageDownloader(max_file_size=1024)  # 1KB
        
        # Should reject large files
        assert downloader.download(...) is False

@pytest.mark.integration
def test_full_pipeline(db_session):
    """End-to-end test of data pipeline."""
    # Setup
    posts = create_test_posts(5)
    
    # Run
    save_hot_posts_to_db()
    fetch_post_data()
    populate_comments()
    
    # Verify
    assert db_session.query(Post).count() == 5
    assert db_session.query(Comment).count() > 0
```

---

## Summary: Priority Order

| Priority | Issue | File | Impact |
|----------|-------|------|--------|
| **HIGH** | N+1 queries | download.py | 100x performance gain |
| **HIGH** | Configuration externalization | constants.py | Dev/prod flexibility |
| **HIGH** | Type hints & data classes | download.py, posts.py | Better maintainability |
| **HIGH** | Persistent caching | data_population.py | Reduced API calls |
| **MEDIUM** | Database migrations | database.py | Deployment safety |
| **MEDIUM** | Image download resilience | download.py | Better reliability |
| **MEDIUM** | API client abstraction | data_population.py | Testability |
| **MEDIUM** | Bulk operations | posts.py | Better performance |
| **LOW** | Filter extensibility | data_population.py | Future-proofing |
| **LOW** | Custom exceptions | All files | Better debugging |
| **LOW** | Test coverage | All | Confidence |

---

## Implementation Timeline

**Phase 1 (Week 1):** Configuration, type hints, data classes
**Phase 2 (Week 2):** Database migrations, API client
**Phase 3 (Week 3):** Query optimization, caching, image downloader
**Phase 4 (Week 4):** Testing, error handling, documentation

Each phase should be implemented with corresponding tests and documentation updates.
