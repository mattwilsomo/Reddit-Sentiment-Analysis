# Reddit sentiment ananlysis 

## Idea

- Project will scrape popular investing subreddits and will find changes in sentiment analysis 
- Will compare the changes in sentiment to changes in the stock price 
- Prediction is to see a positive correlation

## Structure

- **scraper.js will scrape the subreddit for the posts, comments and replies**
- **This will then be stored in a postgresql dataset**

<details>
  <summary>Click to expand</summary>
  <h3> The data will be kept in two tables </h3>
  <strong>Table 1</strong>
  raw_posts: id, author, content, timestamp, parent_id
  <strong>Table 2</strong>
  processed_data: id, author, ticker_symbol, sentiment score 
</details>

- **Analyse in the python script**
<details>
<summary>Click to expand</summary>

<ul>
  <li>Use Regular Expressions or otherwise to identify the ticker</li>
  <li>Use VADER to run a sentiment analysis on the body of the text</li>
  <li>Write findings to the second table of the database</li>
</ul>

</details>


- **Compare the changes in the sentiment to historical changes in the stock price**


