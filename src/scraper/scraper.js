require("dotenv").config();
const Snoowrap = require("snoowrap");

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

    const posts = await r.getSubreddit("pennystocks").getHot({ limit: 1 });

    for (const post of posts) {
        console.log("POST TITLE:\n ", post.title, "\n");
        allPostsData.push({
            id: post.id,
            title: post.title,
            body: post.body,
            author: post.author.name,
            created_utc: post.created_utc,
        });

        // Await the comments and add them to our local list
        const commentsFromThisPost = await fetchCommentsForPost(post.id);
        console.log(`Found ${commentsFromThisPost.length} comments for post ${post.id}`);
        
        // Use concat or the spread operator to add the new comments
        allCommentsCollected = allCommentsCollected.concat(commentsFromThisPost);
    }

    // This final log will now work reliably
    console.log("\n=== FINAL RESULTS ===");
    console.log("Total comments collected:", allCommentsCollected.length);
    console.log("Sample of first 3 comments:", allCommentsCollected.slice(0, 3));
    
    // You can also return the final list if you need to use it elsewhere
    return allCommentsCollected;
}

// This function now ONLY fetches comments and returns them.
// It no longer knows about any global list.
async function fetchCommentsForPost(postId) {
    try {
        const submission = await r.getSubmission(postId).fetch();
        
        // expandReplies can be heavy, fetching all might not be necessary
        // but keeping your original logic.
        await submission.expandReplies({ depth: Infinity, limit: Infinity });

        // Recursive helper function to flatten the comment tree
        function walkAndCollect(comments) {
            let collected = [];
            comments.forEach(comment => {
                if (comment && comment.body && !/I am a bot/.test(comment.body)) {
                    collected.push({
                        id: comment.id,
                        body: comment.body,
                        author: comment.author ? comment.author.name : '[deleted]',
                        created_utc: comment.created_utc,
                        parent_id: comment.parent_id
                    });
                }
                if (comment && comment.replies && comment.replies.length > 0) {
                    collected = collected.concat(walkAndCollect(comment.replies));
                }
            });
            return collected;
        }

        const commentList = walkAndCollect(submission.comments);
        console.log(`Returning ${commentList.length} comments from fetchCommentsForPost.`);
        return commentList; // Return the result

    } catch (error) {
        console.error("An error occurred while fetching comments: ", error);
        return []; // Return an empty array on error to prevent crashes
    }
}

// Start the process
fetchPostsAndComments();