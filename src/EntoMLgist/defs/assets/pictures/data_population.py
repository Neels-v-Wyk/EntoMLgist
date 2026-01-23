import requests
import dagster as dg
from time import sleep
from sqlmodel import Session, select
from EntoMLgist.defs.assets.pictures.constants import SUBREDDIT, DEFAULT_USER_AGENT, POST_CRAWL_DELAY, POST_TOP_COMMENTS_NUM, MAX_BACKOFF
from EntoMLgist.models.reddit import RedditPost, RedditComment
from EntoMLgist.models.database import Post, Comment
from requests.exceptions import JSONDecodeError

def get_posts_from_db(session: Session) -> list[Post]:
    """Fetch all posts from database using SQLModel."""
    statement = select(Post)
    posts = session.exec(statement).all()
    return list(posts)

def retrieve_post_data(post_id: str, backoff: float = 1.0, max_backoff: float = MAX_BACKOFF) -> requests.Response:
    # TODO: Extract URL building and header creation to separate functions for reusability and testability
    url = f"https://www.reddit.com/r/{SUBREDDIT}/comments/{post_id}.json"
    headers = {'User-Agent': DEFAULT_USER_AGENT}
    backoff_duration = backoff

    r = requests.get(url, headers=headers, timeout=10)
    if r.status_code == 429:
        # Use exponential backoff for rate limiting, but cap at max_backoff
        if backoff_duration >= max_backoff:
            raise Exception(f"Max retries exceeded for post {post_id} due to rate limiting")
        sleep(backoff_duration)
        return retrieve_post_data(post_id, backoff=min(backoff_duration * 1.4, max_backoff))
    elif r.status_code == 404:
        return r  # Post not found, handle accordingly elsewhere
    elif r.status_code != 200:
        raise Exception(f"Failed to retrieve post {post_id}: {r.status_code}")

    return r

def extract_comments(comments_data, post_id):
    """Extracts top-level RedditComment objects from the comments data.
    
    Filters out:
    - AutoModerator comments
    - Replies to other comments (only top-level comments are included)
    """
    # TODO: Add comprehensive logging instead of silent print(); consider custom exceptions for invalid comments
    # TODO: Make AutoModerator filter configurable; consider parameterizing comment filters for extensibility
    comments = []
    for comment in comments_data:
        if comment['kind'] != 't1':
            continue
        comment_data = comment['data']
        try:
            # Skip AutoModerator comments
            author = comment_data.get('author', '')
            if author == 'AutoModerator':
                continue
            
            reddit_comment = RedditComment(
                parent_post_id=post_id,
                comment_id=comment_data.get('id'),
                body=comment_data.get('body', ''),
                upvotes=comment_data.get('ups', 0)
            )
            comments.append(reddit_comment)            
        except KeyError as e:
            # Log and skip invalid comments
            print(f"Skipping invalid comment due to missing key: {e}")
    return comments

def log_response_details(context, post_id, response):
    """Logs details of the HTTP response for debugging."""
    context.log.debug(f"Response status code for post {post_id}: {response.status_code}")
    context.log.debug(f"Response headers for post {post_id}: {response.headers}")
    context.log.debug(f"Response content for post {post_id}: {response.text[:500]}...")

@dg.asset(required_resource_keys={"db_session"}, deps=["save_hot_posts_to_db"])
def fetch_post_data(context: dg.AssetExecutionContext) -> dict:
    """Fetches and caches raw post JSON data for all posts in the database."""
    # TODO: Implement disk/persistent caching layer to avoid refetching, add cache invalidation strategy
    session: Session = context.resources.db_session
    posts = get_posts_from_db(session)
    
    post_data_cache = {}
    for post in posts:
        context.log.info(f"Fetching post data for {post.post_id}")
        try:
            response = retrieve_post_data(post.post_id)
            if response.status_code == 404:
                context.log.warning(f"Post {post.post_id} not found (404). Skipping.")
                continue
            elif response.status_code != 200:
                context.log.warning(f"Failed to retrieve post {post.post_id}: HTTP {response.status_code}")
                continue
            
            post_data_cache[post.post_id] = response.json()
            sleep(POST_CRAWL_DELAY)
        except Exception as e:
            context.log.error(f"Error fetching data for post {post.post_id}: {e}")
    
    context.log.info(f"Fetched data for {len(post_data_cache)} posts")
    return post_data_cache

@dg.asset(required_resource_keys={"db_session"})
def populate_post_upvotes(context: dg.AssetExecutionContext, fetch_post_data: dict):
    # TODO: Extract JSON data parsing to helper function, add defensive checks for missing keys with sensible defaults
    session: Session = context.resources.db_session
    posts = get_posts_from_db(session)

    for post in posts:
        if post.post_id not in fetch_post_data:
            context.log.warning(f"No cached data for post {post.post_id}")
            continue
        
        data = fetch_post_data[post.post_id]
        post_data = data[0]['data']['children'][0]['data']
        post.upvotes = post_data.get('ups', 0)

        context.log.info(f"Post {post.post_id} has {post.upvotes} upvotes")
        
        session.add(post)
    
    session.commit()

@dg.asset(required_resource_keys={"db_session"})
def populate_comments(context: dg.AssetExecutionContext, fetch_post_data: dict):
    """Populates up to POST_TOP_COMMENTS_NUM comments for each post in the database"""
    session: Session = context.resources.db_session
    posts = get_posts_from_db(session)

    for post in posts:
        context.log.info(f"Retrieving comments for post {post.post_id}")

        try:
            if post.post_id not in fetch_post_data:
                context.log.warning(f"No cached data for post {post.post_id}. Skipping.")
                continue
            
            data = fetch_post_data[post.post_id]

            try:
                comments_data = data[1]['data']['children']
                comments = extract_comments(comments_data, post.post_id)
            except (KeyError, IndexError) as e:
                context.log.error(f"Error extracting comments for post {post.post_id}: {e}")
                continue

            # Get only top N comments, filtering by upvotes
            top_comments = sorted(comments, key=lambda x: x.upvotes, reverse=True)[:POST_TOP_COMMENTS_NUM]

            for comment in top_comments:
                # Use SQLModel's merge to handle upserts
                db_comment = Comment(
                    comment_id=comment.comment_id,
                    parent_post_id=comment.parent_post_id,
                    body=comment.body,
                    upvotes=comment.upvotes
                )
                session.merge(db_comment)

            context.log.info(f"Inserted comments for post {post.post_id}")
            sleep(POST_CRAWL_DELAY)

        except Exception as e:
            context.log.error(f"Unexpected error while processing post {post.post_id}: {e}")

    session.commit()
