# EntoMLgist
Insect detection/classification using YOLO, Visual Transformers, and NLP, using Dagster for continous data ingestion and training on data from Reddit's /r/whatisthisbug.

This repo is currently under construction.

## WIP goals/outline
- [ ] Set up dockerized Dagster, every step below is a DAG 
- [ ] Retrieve images from reddit's "what is this bug" subreddit
- [ ] Retrieve comments from the above as well
- [ ] Store already gotten comments and post IDs in a DB so we don't get duplicates in the future
- [ ] Create a sample training data set which is manually labeled for insect identification (location only)
- [ ] Train YOLO to find insects
- [ ] Use NLP to extract insect names from top comments on posts, for use in classification training
- [ ] Set up a ViT to classify insects based on cropped images from YOLO and text data
- [ ] Continously improve the model on new posts

## Why even do this?
I got so gosh darn tired of every second post on the subreddit /r/whatisthisbug going "is this a bedbug??" and it being a very clear example of a bedbug or german cockroach, so I decided to do something about it. If this project pans out and has a high degree of accuracy, I can make a reddit bot to try and answer posts early, so that everyone can hopefully see some cool bugs again.
