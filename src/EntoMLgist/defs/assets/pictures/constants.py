SUBREDDIT = "whatisthisbug"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
POST_CRAWL_DELAY = 1 # how many seconds to wait between iterating posts, to avoid rate limiting
MAX_BACKOFF = 60  # maximum backoff time in seconds for rate limiting
TOTAL_PICTURES = 100 # total number of pictures to fetch per run
POST_TOP_COMMENTS_NUM = 3  # number of most upvoted top-level comments to fetch per post
IMAGE_DOWNLOAD_UPVOTE_THRESHOLD = 5  # minimum upvotes required for an image to be downloaded
IMAGE_COMMENT_COUNT_THRESHOLD = 3  # minimum number of comments required for a post to be considered for training data
IMAGE_DOWNLOAD_PATH = "./downloads/images"
# TODO: Put these global variables somewhere better
