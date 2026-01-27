import tickers
from db import connection
from pathlib import Path

insert_comment = """
INSERT INTO comment_tickers(
    comment_id, parent_id, post_id, ticker, detected_by, confidence, context_snippet,
     inferred_from, author, created_utc
    )
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(comment_id) DO NOTHING;
"""

insert_post = """
INSERT INTO post_tickers(
    post_id, ticker, detected_by, confidence, body,  author, created_utc
)
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(post_id) DO NOTHING;
"""

def gettickers():
    comments, posts = tickers.process_db()
    return (comments, posts)

def querys():
    """
    Gets all the matches from the data base
    Creates new tables (post_tickers and comment_tickers) if they do not exists yet
    populates both tables with the matches data
    Args:
        None
    Returns:
        None
    """
    comments, posts = tickers.process_db()
    sql_comments = Path("queries/comment_tickers.sql").read_text()
    sql_posts = Path("queries/post_ticker.sql").read_text()
    conn = connection()
    curr = conn.cursor()
    curr.execute(sql_posts)
    curr.execute(sql_comments)
    for comment in comments:
        id, parent_id, post_id, body, author, created_utc = comment["comment"]
        ticker = comment["match_details"]["ticker"]
        mention_kind = comment["match_details"]["kind"]
        confidence = comment["match_details"]["score"]
        inferred_from = comment["match_details"]["inferred_from"]
        curr.execute(sql_comments,(
            id, parent_id, post_id, ticker, mention_kind, confidence, body[:300], inferred_from, author, created_utc
        ))
    for post in posts:
        ...
# INSERT INTO comment_tickers(
#     comment_id, parent_id, post_id, ticker, detected_by, confidence, context_snippet,
#      inferred_from, author, created_utc
#     )

