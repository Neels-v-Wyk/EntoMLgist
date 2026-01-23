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
# Hashing configuration
IMAGE_ID_HASH_ALGORITHM = "sha256"  # algorithm for generating image IDs; supported: 'sha256', 'sha1', 'md5'
IMAGE_ID_HASH_LENGTH = 8  # length of hash to use for image ID (characters)
# TODO: Use environment variables or config file to externalize these magic numbers for different environments
# TODO: Add validation/type hints for all constants
