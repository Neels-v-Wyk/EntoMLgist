# This downloads a bunch of pictures from the subreddit in order to use them down the line
import requests
import dagster as dg
from EntoMLgist.models.reddit import RedditPost
from EntoMLgist.defs.assets.reddit.constants import SUBREDDIT, DEFAULT_USER_AGENT, POST_CRAWL_DELAY, TOTAL_PICTURES


def get_posts(limit: int = 100) -> requests.Response:
    # TODO: Implement connection pooling and add request timeout/retry decorator
    url = f"https://www.reddit.com/r/{SUBREDDIT}/hot.json?limit={limit}"
    headers = {'User-Agent': DEFAULT_USER_AGENT}
    
    try:
        r = requests.get(url, headers=headers, timeout=30)
        return r
    except requests.RequestException as e:
        # Create a mock response object with error info
        class MockResponse:
            status_code = 500
            text = f"Request failed: {e}"
            def json(self):
                raise ValueError(f"Request failed: {e}")
        
        return MockResponse()

def filter_posts(posts: requests.Response) -> list[RedditPost]:
    # TODO: Define return type as list[RedditPost] in docstring, add filtering logic (score, age, etc) beyond parsing
    # Sort posts into {post_id, title} from raw html
    filtered_posts = []
    
    try:
        data = posts.json()
        for post in data['data']['children']:
            post_data = post['data']
            post_id = post_data.get('id')
            title = post_data.get('title')
            
            if not post_id or not title:
                continue
                
            reddit_post = RedditPost(post_id=post_id, title=title)
            filtered_posts.append(reddit_post)
    except Exception as e:
        # If JSON parsing fails, log the error and return empty list
        print(f"Error parsing Reddit API response: {e}")
        print(f"Response content: {posts.text[:1000]}...")
        return []
    
    return filtered_posts

def get_hot_posts(context: dg.AssetExecutionContext) -> list[RedditPost]:
    context.log.info(f"Fetching {TOTAL_PICTURES} posts from r/{SUBREDDIT}")
    posts = get_posts(limit=TOTAL_PICTURES)

    if posts.status_code != 200:
        context.log.error(f"Failed to retrieve posts: HTTP {posts.status_code}")
        context.log.error(f"Response: {posts.text[:500]}...")
        return []

    filtered_posts = filter_posts(posts)
    context.log.info(f"Retrieved {len(filtered_posts)} posts from r/{SUBREDDIT}")
    return filtered_posts

@dg.asset(required_resource_keys={"db_session"}, deps=["create_database_tables"])
def save_hot_posts_to_db(context: dg.AssetExecutionContext):
    # TODO: Use bulk insert for better performance, handle duplicate key errors gracefully with upsert
    from sqlmodel import Session
    from EntoMLgist.models.database import Post
    
    try:
        posts = get_hot_posts(context)
        
        if not posts:
            context.log.warning("No posts retrieved, skipping database save")
            return
        
        session: Session = context.resources.db_session
        
        context.log.info(f"Attempting to save {len(posts)} posts to database")
        
        saved_count = 0
        for post in posts:
            try:
                db_post = Post(
                    post_id=post.post_id,
                    title=post.title,
                    upvotes=0
                )
                session.merge(db_post)
                saved_count += 1
            except Exception as e:
                context.log.error(f"Failed to save post {post.post_id}: {e}")
        
        session.commit()
        context.log.info(f"Successfully saved {saved_count} posts to database")
        
    except Exception as e:
        context.log.error(f"Unexpected error in save_hot_posts_to_db: {e}")
        raise

    return dg.MaterializeResult(
        metadata={"saved_posts_count": saved_count}
    )
