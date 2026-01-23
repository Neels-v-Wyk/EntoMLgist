# This contains all Reddit related models and functions

class RedditComment:
    def __init__(self, parent_post_id: str, comment_id: str, body: str, upvotes: int = 0):
        self.parent_post_id = parent_post_id
        self.comment_id = comment_id
        self.body = body
        self.upvotes = upvotes
    
    def to_dict(self) -> dict:
        return {
            "parent_post_id": self.parent_post_id,
            "comment_id": self.comment_id,
            "body": self.body,
            "upvotes": self.upvotes
        }

class RedditPost:
    def __init__(self, post_id: str, title: str, image_urls: list = [], comments: list = [RedditComment], upvotes: int = 0):
        self.post_id = post_id
        self.title = title
        self.image_urls = image_urls
        self.comments = comments
        self.upvotes = upvotes
    
    def add_image_url(self, url: str):
        self.image_urls.append(url)
    
    def add_comment(self, comment: str):
        self.comments.append(comment)

    def to_dict(self) -> dict:
        return {
            "post_id": self.post_id,
            "title": self.title,
            "image_urls": self.image_urls,
            "comments": self.comments,
            "upvotes": self.upvotes
        }
