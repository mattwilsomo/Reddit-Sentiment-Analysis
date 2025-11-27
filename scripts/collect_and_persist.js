process.on('unhandledRejection', () => {});
process.on('uncaughtException', () => {});

require('dotenv').config();
require('../src/envCheck');

const pool = require('../src/db/connection')
const { batchUpsertPosts, batchUpsertComments} = require('../src/db/persist');
const {fetchPostsAndComments} = require('../src/scraper/scraper');


async function runCollection(subreddit = "pennystocks", limit = 10, postChunkSize = 50, commentChunkSize = 200){
    console.log("Starting collection process: ", new Date().toISOString())
    try{
        const results = await fetchPostsAndComments(subreddit, limit);

        console.log(`Fetched posts: ${results.posts.length}\n Fetched Comments ${results.comments.length}`)
        
        if(results.posts.length> 0){
            console.log(`Persisting posts in chunks of ${postChunkSize}...`);
            await batchUpsertPosts(results.posts, postChunkSize);
            console.log('✅ Posts persisted');
        } else{
            console.log('No posts to persist')
        }

        if (results.posts.length > 0){
            console.log(`Persisting comments in chunks of ${commentChunkSize}...`);
            await batchUpsertComments(results.comments, commentChunkSize);
            console.log('✅ Comments persisted');
        } else{
            console.log('No comments to persist.')
        }
        // const p = await pool.query('SELECT *FROM posts')
        // console.log('post rows:', p.rows);
        console.log('Collection run finished: ', new Date().toISOString())
    }catch(err) {
        console.error('Collection Run failed', err);
        throw err;
    }finally{
        try{
            await pool.end();
            console.log("DB pool closed")
        } catch(err){
            console.warn('Error in closing DB pool', err)
        }
    }
}

if (require.main === module) {
  runCollection().catch(err => {
    console.error('Fatal error in collect_and_persist:', err);
    process.exit(1);
  });
}

module.exports = { runCollection };