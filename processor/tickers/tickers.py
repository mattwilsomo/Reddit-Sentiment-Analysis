from db import connection
from psycopg2 import Error as psyError
import re
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial

"""We will load in the scraped file from the reddit posts and comments, we will then look for any tickers mentioned in the comment
This will be done using:
    - regular expressions
    - Machine learning


    """

def load_tickers (path):
    # file: contains one ticker per line 
    df = pd.read_csv(path, usecols=["Ticker"]) # The tickers in all major US exchanges
    return df

# We will identify tickers using regular expressions 
Dollar_RE = re.compile(r'\$([A-Za-z]{1,5})\b')
Upper_RE = re.compile(r'([A-Z]{2,5})\b')
Symbol_RE = re.compile(r'\b(?:ticker|symbol)[:\s]*([A-Za-z]{1,6})\b', re.I)
Lower_RE = re.compile(r"\b[a-z]{3,5}\b")

# looks for context around the ticker to build confindence
context_RE = re.compile(r'\b(buy|sell|shares|short|long|stock|IPO|earnings|dividend|split|bought|sold|play|position|trading|trade|cheap|M&A|gain|moving|moon|holding|squeeze|hold|watch|dip|volume|catalyst|pump|dump|earnings|bullish|undervalued|bearish)\b', re.I)

# blacklisted common financial terms so they do not trigger false positives, plus other terms frequentyly capitalised
Redlist = {"GUYS", "MOON", "US", "UK", "EBIT", "EBITDA", "UP","CAGR", "FCF", "ROE", "ROI", "ROIC", "EV", "NI", "PEG", "EU", "GPT", "AI", "IT", "LFG", "CEO", "CFO", "NASDAQ", "NYSE", "LSE", "ASX", "TSE","SSE", "SEHK","TSX"}

#checks for a numerical number, optional currency sign, optional decimal points
Numerical_RE = re.compile(r"\b(?:\$|£)?\d+(?:\.\d+)?\b")

#ticker file 
path = "./tickers.csv"


def extract_candidates(text, ticker_set, allow_lowercase = False):
    # text is the comment or post text and tickers is dataframe of tickers 

    candidates = set()
    
    
    for m in Dollar_RE.finditer(text):
        t = m.group(1).upper() # the capture group from the text eg: TSLA from $TSLA
        if t in ticker_set:
            candidates.add((t, "dollar",m.start(), m.group(0)))

    for m in Symbol_RE.finditer(text):
        t = m.group(1).upper()
        if t in ticker_set:
            candidates.add((t, "symbol_prefix", m.start(), m.group(0)))
    
    for m in Upper_RE.finditer(text):
        t = m.group(0).upper()
        if t in ticker_set:
            # filter any words that are in the redlist
            if t in Redlist:
                continue
            candidates.add((t, "allcaps", m.start(), m.group(0)))
    
    if allow_lowercase and context_RE.search(text) or Numerical_RE.search(text):
        for m in Lower_RE.finditer(text):
            t = m.group(0)
            up = t.upper()
            if up in ticker_set:
                candidates.add((up, "lowercase_with_context", m.start(), m.group(0)))
    return list(candidates)

def score_potential(text, candidate, post = False):
    #candidate is a list of all the qualities of a ticker in a comment
    ticker, kind, pos, raw = candidate
    # scored between 0 and 1
    score = 0.0
    if kind == "dollar":
        score += 0.91
    elif kind == "symbol_prefix":
        score += 0.91
    elif kind == "allcaps":
        score += 0.7
        if post:
            score = 0.91
    elif kind == "lowercase_with_context":
        score = 0 

        
    if context_RE.search(text):
        score += 0.4
    
    window = text[max(0,pos-50):pos+50]
    if Numerical_RE.search(window):
        score += 0.5
    #return min(score, 1.0)
    return score

def process_text(text, ticker_set,*, allow_lowercase = False, threshold = 0.9,post = False):
    candidates = extract_candidates(text, ticker_set, allow_lowercase)
    results = []
    for c in candidates:
        valid = False
        score = score_potential(text,c, post)
        if score > threshold:
            valid = True
        if valid:
            results.append({"ticker": c[0], "kind": c[1], "score": score, "snippet": text[:200]})
    return results

def lounge_id():
    conn = connection()
    curr = conn.cursor()

    curr.execute("""SELECT id FROM posts
                 WHERE title = 'The Lounge'""")
    
    ids = curr.fetchall()

    return ids



def propogate_for_comment(comment_row, matches_com, matches_post, tree=None):
    """
    comment_row: (comment_id, parent_id, post_id, body)
    matches_com: list of comment-level matches
                 [{"comment":[id, body], "match_details": {...}}]
    matches_post: list of post-level matches
                  [{"post":[title, post_id], "match_details": {...}}]
    tree: ordered list of ancestor comment_ids, e.g. [parent, grandparent, greatgrandparent]
    """

    comment_id, parent_id, post_id, body = comment_row

    DECAY = 0.8
    MAX_DEPTH = 3
    PARENT_CONF_LIMIT = 0.9
    CHILD_CONF_LIMIT = 0.72

    # ---------- 1) COMMENT-LEVEL PROPAGATION ----------
    if tree:
        for depth, ancestor_id in enumerate(tree, start=1):
            if not ancestor_id or depth > MAX_DEPTH:
                break

            # find ancestor match
            parent_match = None
            for match in matches_com:
                if match["comment"][0] == ancestor_id:
                    parent_match = match
                    break

            if parent_match:
                parent_score = parent_match["match_details"]["score"]
                if parent_score >= PARENT_CONF_LIMIT:
                    child_conf = round(parent_score * (DECAY ** depth),3)
                    if child_conf > CHILD_CONF_LIMIT:
                        return {
                            "comment": [comment_id, body],
                            "match_details": {
                                "ticker": parent_match["match_details"]["ticker"],
                                "kind": "propagated_comment",
                                "score": child_conf,
                                "snippet": body[:300],
                                "inferred_from": ancestor_id,
                                "hops": depth
                            }
                    }

    # ---------- 2) POST-LEVEL PROPAGATION ----------
    for post_match in matches_post:
        
        if post_match["post"][1] == post_id:
            parent_score = post_match["match_details"]["score"]
            if parent_score >= PARENT_CONF_LIMIT:
                child_conf = round(parent_score * DECAY,3)
                if child_conf > CHILD_CONF_LIMIT:
                    return {
                        "comment": [comment_id, body],
                        "match_details": {
                            "ticker": post_match["match_details"]["ticker"],
                            "kind": "propagated_post",
                            "score": child_conf,
                            "snippet": body[:300],
                            "inferred_from": post_id,
                            "hops": "post"
                        }
                }

    return None


def build_ancestor_tree(comment_id, parent_map, max_depth=3):
    """
    Returns list of ancestor comment_ids in order:
    [parent, grandparent, greatgrandparent]
    """
    tree = []
    current = comment_id
    depth = 0

    while depth < max_depth:
        parent = parent_map.get(current, (None, None))[0]
        if not parent:
            break
        tree.append(parent)
        current = parent
        depth += 1

    return tree


def process_posts_from_db(batchSize = 100):
    conn = connection()

    curr = conn.cursor()
    tickers = load_tickers(path)
    ticker_set = set(tickers["Ticker"])
    matched_ls = []
    try:
        curr.execute(
        """SELECT title, id FROM posts
        WHERE NOT title = 'The Lounge';""")
        with Pool(processes=max(1,cpu_count()-1)) as pool:
            while True:
                rows = curr.fetchmany(batchSize)
                if not rows:
                    break
                
                #parrallel processing for speed, args is (comment text, ticker csv)
                fn = partial(process_text, post=True)
                args = [(r[0], ticker_set) for r in rows]
                #bucketed is the result from process_comment(comment_text, ticker_set)
                bucketed = pool.starmap(fn, args)

                #matches is all the matches found in the post    
                for post, matches in zip(rows, bucketed):
                    if matches:
                        # print(f"Found matches in: {post[0][:300]}...\n Ticker: {matches[0]["ticker"]} Score: {matches[0]["score"]}\n ")
                        matched_ls.append({"post":[post[0], post[1]], "match_details": matches[0]})

    except psyError as e:
        print("Post database error ", e)
    
    
    finally:
        curr.close()
        conn.close()
    

    return matched_ls

def process_db( batchSize = 500):
    conn = connection()

    curr= conn.cursor()

    tickers = load_tickers(path)
    ticker_set = set(tickers["Ticker"])

    #This will get all the matches from the posts to be used later in the propogation
    matches_posts = process_posts_from_db()

    try:
        count = 0
        curr.execute("SELECT id, parent_id, post_id,  body FROM comments")
        matched_ls = []
        with Pool(processes=max(1,cpu_count()-1)) as pool:
            while True:
                rows = curr.fetchmany(batchSize)
                if not rows:
                    break
                
                #parrallel processing for speed, args is (comment text, ticker csv)

                args = [(r[3], ticker_set) for r in rows]
                #bucketed is the result from process_comment(comment_text, ticker_set)
                bucketed = pool.starmap(process_text, args)

                # comment_id -> (parent_id, post_id)
                parent_map = {
                    r[0]: (r[1], r[2])
                    for r in rows
                }


                #matches is a list of dictionaries of all the matches in the comment eg [{'ticker': 'AAPL', 'kind': 'dollar', 'score': 0.95, 'snippet': '...'},]]    
                for row, matches in zip(rows, bucketed):
                    comment_id, parent_id, post_id, comment = row
                    if matches:
                        print(f"Found matches in: {comment[0][:300]}...\n Ticker: {matches[0]["ticker"]} Score: {matches[0]["score"]}\n {matches}\n", )
                        matched_ls.append({"comment": [comment_id,comment], "match_details": matches[0]})
                    else:
                        tree = build_ancestor_tree(comment_id, parent_map)

                        propagated = propogate_for_comment(
                            comment_row=row,
                            matches_com=matched_ls,
                            matches_post=matches_posts,
                            tree=tree
                        )

                        if propagated:
                            matched_ls.append(propagated)
                            for matched in matched_ls:
                                if matched["match_details"]["kind"] == "propagated_comment" or  matched["match_details"]["kind"] == "propagated_post":
                                     print(f"Found matches in: {comment[0][:300]}...\n Ticker: {matched["match_details"]["ticker"]} Score: {matched["match_details"]["score"]}\n {matched}\n", )                

        
    except psyError as e:
        print("Database error: ",e)
   

    finally:
        curr.close()
        conn.close()
    return matched_ls



       
if __name__ == "__main__":
    process_db()
