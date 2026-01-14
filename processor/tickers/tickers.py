from db import connection
from psycopg2 import Error as psyError
import re
import pandas as pd
from multiprocessing import Pool, cpu_count

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
context_RE = re.compile(r'\b(buy|sell|shares|short|long|stock|IPO|earnings|dividend|split|bought|sold|play|position|trading|trade|cheap|M&A|gain|moving|moon|holding|squeeze|hold|watch|dip|volume|catalyst|pump|dump|earnings)\b', re.I)

# blacklisted common financial terms so they do not trigger false positives, plus other terms frequentyly capitalised
Redlist = {"GUYS", "MOON", "US", "UK", "EBIT", "EBITDA", "UP","CAGR", "FCF", "ROE", "ROI", "ROIC", "EV", "NI", "PEG", "EU", "GPT", "AI", "IT", "LFG"}

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

def score_potential(text, candidate):
    #candidate is a list of all the qualities of a ticker in a comment
    ticker, kind, pos, raw = candidate
    # scored between 0 and 1
    score = 0.0
    if kind == "dollar":
        score += 0.9
    elif kind == "symbol_prefix":
        score += 0.9
    elif kind == "allcaps":
        score += 0.7
    elif kind == "lowercase_with_context":
        score = 0 

        
    if context_RE.search(text):
        score += 0.4
    
    window = text[max(0,pos-50):pos+50]
    if Numerical_RE.search(window):
        score += 0.5
    #return min(score, 1.0)
    return score

def process_comment(comment_text, ticker_set, allow_lowercase = False, threshold = 0.9):
    candidates = extract_candidates(comment_text, ticker_set, allow_lowercase)
    results = []
    
    for c in candidates:
        valid = False
        score = score_potential(comment_text,c)
        if score > threshold:
            valid = True
        if valid:
            results.append({"ticker": c[0], "kind": c[1], "score": score, "snippet": comment_text[:200]})
    return results



def process_comments_from_db( batchSize = 100):
    conn = connection()

    curr= conn.cursor()

    tickers = load_tickers(path)
    ticker_set = set(tickers["Ticker"])
    try:
        count = 0
        curr.execute("SELECT body FROM comments")
        with Pool(processes=max(1,cpu_count()-1)) as pool:
            while True:
                rows = curr.fetchmany(batchSize)
                if not rows:
                    break
                
                #parrallel processing for speed, args is (comment text, ticker csv)

                args = [(r[0], ticker_set) for r in rows]
                #bucketed is the result from process_comment(comment_text, ticker_set)
                bucketed = pool.starmap(process_comment, args)

                    
                for row, matches in zip(rows, bucketed):
                    if matches:
                        print(f"Found matches in: {row[0][:300]}...\n Ticker: {matches[0]["ticker"]} Score: {matches[0]["score"]}\n {matches}")
                        count +=1
            print(count)
        
    except psyError as e:
        print("Database error: ",e)
   

    finally:
        curr.close()
        conn.close()


       
if __name__ == "__main__":
    process_comments_from_db()
