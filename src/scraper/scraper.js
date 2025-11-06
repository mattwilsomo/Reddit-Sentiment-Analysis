require("dotenv").config();
require('../envCheck');
const Snoowrap = require("snoowrap");
const { walkAndCollect } = require("./utils.js"); 

// Initialize Snoowrap with Reddit app credentials
const r = new Snoowrap({
    userAgent: process.env.USER_AGENT,
    clientId: process.env.CLIENT_ID,
    clientSecret: process.env.CLIENT_SECRET,
    username: process.env.REDDIT_USER,
    password: process.env.REDDIT_PASS
});

// Main function to orchestrate everything

async function fetchPostsAndComments() {
    // This list is now local to the main function
    let allCommentsCollected = []; 
    const allPostsData = [];
    const subreddit = "pennystocks";

    const posts = await r.getSubreddit(subreddit).getHot({ limit: 3 });

    for (const post of posts) {
        console.log("POST TITLE:\n ", post.title, "\n");
        allPostsData.push({
            id: post.name,
            title: post.title,
            body: post.body,
            author: post.author.name,
            created_utc: post.created_utc,
            score: post.score
        });

        // Await the comments and add them to our local list
        const commentsFromThisPost = await fetchCommentsForPost(post.id);
        console.log(`Found ${commentsFromThisPost.length} comments for post ${post.id}`);
        
        // Use concat or the spread operator to add the new comments
        allCommentsCollected = allCommentsCollected.concat(commentsFromThisPost);

        // add a small delay to avoid hitting rate limits
        await new Promise(resolve => setTimeout(resolve, 2000));
    }

    console.log("\n=== FINAL RESULTS ===");
    console.log("Total comments collected:", allCommentsCollected.length);
    console.log("Sample of first 3 comments:", allCommentsCollected.slice(0, 3));
    
  
    return [allPostsData, allCommentsCollected];
}

// This function fetches comments and returns them.
async function fetchCommentsForPost(postId) {
    let retries = 0;
    const maxRetries = 5;
    const retryDelay = 2000; // 2 seconds, initial delay

    while (retries < maxRetries) {
        try {
            const submission = await r.getSubmission(postId).fetch();

            // Expand replies, to get a deeper comment tree, 4 levels deep, max 5 replies each
            await submission.expandReplies({ depth: 4, limit: 5 });

            // Recursive helper function to flatten the comment tree
            const commentList = walkAndCollect(submission.comments);
            
            console.log(`Returning ${commentList.length} comments from fetchCommentsForPost.`);
            return commentList; // Return the result

        } catch (error) {

            if (error.statusCode === 429) {
                retries++;
                const delay = retryDelay * Math.pow(2,retries); // Exponential backoff
                console.warn(`Rate limit exceeded. Retrying in ${delay/1000} seconds...`)
                await new Promise(resolve => setTimeout (resolve, delay))
        } else{
            console.error("Error fetching comments:", error);
            return []; // Return empty array on other errors
        }
    }
    
    }
    console.error("Max retries reached. Returning empty comment list.");
    return []; // Return empty array if max retries reached
}

// Start the process
fetchPostsAndComments();