require("dotenv").config(); // Load environment variables from .env file
const Snoowrap = require("snoowrap"); // Import Snoowrap Reddit API wrapper

// Create a Snoowrap client using credentials from environment variables
const r = new Snoowrap({
    userAgent: process.env.USER_AGENT,
    clientId: process.env.CLIENT_ID,
    clientSecret: process.env.CLIENT_SECRET,
    username: process.env.REDDIT_USER,
    password: process.env.REDDIT_PASS
});

// Fetch latest posts from the 'pennystocks' subreddit
async function fetchPosts() {
    const posts = await r.getSubreddit("pennystocks").getNew({limit: 5}); // Get 5 newest posts
    
    for(const post of posts){
        console.log("POST TITLE:\n ", post.title, "\n"); // Print post title
        await fetchComments(post.id); // Fetch and print comments for each post
    }
} 

// Fetch and print all comments for a given post
async function fetchComments(postId) {
  try {
    const submission = await r.getSubmission(postId); // Get submission by ID
    await submission.expandReplies({ depth: Infinity, limit: Infinity }); // Expand all replies

    // Recursively walk through all comments and print them
    function walk(comments) {
      comments.forEach(comment => {
        if (comment.body) {
          // Skip auto-generated bot comments
          if (/I am a bot, and this comment was made automatically./.test(comment.body)) {
            return;
          }
          console.log(comment.id, ":", comment.body); // Print comment ID and body
        }

        // If there are replies, walk through them recursively
        if (comment.replies && comment.replies.length > 0) {
          walk(comment.replies);
        }
      });
    }

    walk(submission.comments); // Start walking from top-level comments

  } catch (error) {
    console.error("An error occurred: ", error); // Handle errors
  }
}
fetchPosts(); // Start the fetching process