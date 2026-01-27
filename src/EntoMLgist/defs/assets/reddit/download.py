import requests
import dagster as dg
import hashlib
import os
import html
from sqlmodel import Session, select
from EntoMLgist.defs.assets.pictures.data_population import retrieve_post_data
from EntoMLgist.defs.assets.pictures.constants import DEFAULT_USER_AGENT, IMAGE_DOWNLOAD_PATH, IMAGE_DOWNLOAD_UPVOTE_THRESHOLD, IMAGE_COMMENT_COUNT_THRESHOLD, IMAGE_ID_HASH_ALGORITHM, IMAGE_ID_HASH_LENGTH
from EntoMLgist.models.database import Post, ImageUrl

def generate_image_id(url: str) -> str:
    """Generate a short hash ID for an image URL using configurable algorithm.
    
    Args:
        url: The image URL to hash
    
    Returns:
        A hash string of length IMAGE_ID_HASH_LENGTH using IMAGE_ID_HASH_ALGORITHM
    
    Raises:
        ValueError: If IMAGE_ID_HASH_ALGORITHM is not supported
    """
    supported_algorithms = {'sha256', 'sha1', 'md5'}
    algorithm = IMAGE_ID_HASH_ALGORITHM.lower()
    
    if algorithm not in supported_algorithms:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}. Supported: {supported_algorithms}")
    
    hash_obj = hashlib.new(algorithm)
    hash_obj.update(url.encode())
    return hash_obj.hexdigest()[:IMAGE_ID_HASH_LENGTH]

def get_extension_from_url(url: str) -> str:
    """Extract file extension from URL, handling query parameters."""
    return url.split('.')[-1].split('?')[0]

def get_image_uris_from_response(post_json: dict, post_id: str) -> list:
    """Extract image URIs from Reddit post JSON response."""
    # TODO: Add type hints (list[dict]) and define data class for image URI objects, make extension validation regex configurable
    image_uris = []
    
    try:
        post_data = post_json[0]['data']['children'][0]['data']
        
        # Handle gallery posts with media_metadata (most reliable source)
        if 'media_metadata' in post_data:
            for media_id, media_info in post_data['media_metadata'].items():
                if media_info.get('e') == 'Image' and 's' in media_info:
                    url = media_info['s']['u']
                    extension = get_extension_from_url(url)
                    image_uris.append({'url': url, 'image_id': media_id, 'extension': extension})
        
        # Handle preview images (fallback for single images without media_metadata)
        elif 'preview' in post_data and 'images' in post_data['preview']:
            for idx, image in enumerate(post_data['preview']['images']):
                if 'source' in image and 'url' in image['source']:
                    url = image['source']['url']
                    extension = get_extension_from_url(url)
                    # Only include if it looks like an actual image URL
                    if extension.lower() in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                        image_id = generate_image_id(url)
                        image_uris.append({'url': url, 'image_id': image_id, 'extension': extension})
    
    except Exception as e:
        print(f"Error extracting image URIs from post {post_id}: {e}")
    
    return image_uris

def download_image_from_uri(url: str, local_path: str) -> bool:
    """Download a single image from a URL and save it locally. Returns True if successful."""
    # TODO: Add retry logic with exponential backoff, validate image file integrity (magic bytes/hash), support streaming for large files
    try:
        # Decode HTML entities in the URL (e.g., &amp; -> &)
        url = html.unescape(url)
        
        headers = {
            'User-Agent': DEFAULT_USER_AGENT,
            'Referer': 'https://reddit.com/',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            # create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, 'wb') as f:
                f.write(response.content)
            return True
        else:
            print(f"Failed to download image from {url}: HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"Error downloading image from {url}: {e}")
        return False

@dg.asset(required_resource_keys={"db_session"})
def get_image_uris_from_posts(context: dg.AssetExecutionContext, fetch_post_data: dict):
    """Extracts image URIs from Reddit posts and stores them in the image_urls table."""
    session: Session = context.resources.db_session
    
    # Get all posts
    statement = select(Post)
    posts = session.exec(statement).all()
    
    for post in posts:
        context.log.info(f"Extracting image URIs for post {post.post_id}")
        
        try:
            if post.post_id not in fetch_post_data:
                context.log.warning(f"No cached data for post {post.post_id}. Skipping.")
                continue
            
            post_json = fetch_post_data[post.post_id]
            image_uris = get_image_uris_from_response(post_json, post.post_id)
            
            for image_data in image_uris:
                # Use SQLModel merge for upsert
                image_url = ImageUrl(
                    image_id=image_data['image_id'],
                    parent_post_id=post.post_id,
                    url=image_data['url'],
                    extension=image_data['extension'],
                    downloaded=0
                )
                session.merge(image_url)
            
            session.commit()
            context.log.info(f"Extracted {len(image_uris)} image URIs for post {post.post_id}")
        
        except Exception as e:
            context.log.error(f"Error extracting image URIs for post {post.post_id}: {e}")

@dg.asset(required_resource_keys={"db_session"}, deps=["get_image_uris_from_posts"])
def download_all_pictures(context: dg.AssetExecutionContext):
    """Downloads all pictures from the image_urls table and updates local_path and downloaded flag."""
    session: Session = context.resources.db_session
    
    statement = select(ImageUrl).where(ImageUrl.downloaded == 0)
    images = session.exec(statement).all()
    
    for image in images:
        local_path = f"{IMAGE_DOWNLOAD_PATH}{image.parent_post_id}-{image.image_id}.{image.extension}"
        context.log.info(f"Downloading image {image.image_id} from post {image.parent_post_id}")
        
        if download_image_from_uri(image.url, local_path):
            image.downloaded = 1
            image.local_path = local_path
            session.add(image)
            session.commit()
            context.log.info(f"Image {image.image_id} saved as {image.parent_post_id}-{image.image_id}.{image.extension}")
        else:
            context.log.error(f"Failed to download image {image.image_id}")

@dg.asset(required_resource_keys={"db_session"}, deps=["populate_post_upvotes", "populate_comments", "get_image_uris_from_posts"])
def download_filtered_pictures(context: dg.AssetExecutionContext):
    """Downloads pictures from posts that pass IMAGE_COMMENT_COUNT_THRESHOLD and IMAGE_DOWNLOAD_UPVOTE_THRESHOLD."""
    # TODO: Move comment count filtering to SQL query (GROUP BY + HAVING) to avoid N+1 queries, add batch processing for scalability
    from sqlmodel import func, col
    from EntoMLgist.models.database import Comment
    
    session: Session = context.resources.db_session
    
    # Build query with joins to filter by upvotes and comment count
    statement = (
        select(ImageUrl)
        .join(Post, ImageUrl.parent_post_id == Post.post_id)
        .where(
            Post.upvotes >= IMAGE_DOWNLOAD_UPVOTE_THRESHOLD,
            ImageUrl.downloaded == 0
        )
    )
    
    images = session.exec(statement).all()
    
    # Filter by comment count (need to check separately)
    for image in images:
        # Count comments for this post
        comment_count_stmt = (
            select(func.count(Comment.comment_id))
            .where(Comment.parent_post_id == image.parent_post_id)
        )
        comment_count = session.exec(comment_count_stmt).one()
        
        if comment_count < IMAGE_COMMENT_COUNT_THRESHOLD:
            continue
        
        local_path = f"{IMAGE_DOWNLOAD_PATH}{image.parent_post_id}-{image.image_id}.{image.extension}"
        context.log.info(f"Downloading filtered image {image.image_id} from post {image.parent_post_id}")
        
        if download_image_from_uri(image.url, local_path):
            image.downloaded = 1
            image.local_path = local_path
            session.add(image)
            session.commit()
            context.log.info(f"Image {image.image_id} saved as {image.parent_post_id}-{image.image_id}.{image.extension}")
        else:
            context.log.error(f"Failed to download filtered image {image.image_id}")
