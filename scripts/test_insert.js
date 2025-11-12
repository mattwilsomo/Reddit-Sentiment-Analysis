require('dotenv').config();
const pool = require('../src/db/connection');
const { batchUpsertPosts, batchUpsertComments } = require('../src/db/persist');

(async () => {
  try {
    await batchUpsertPosts([{
      id: 'test_post_1',
      subreddit: 'r/test',
      title: 'Test',
      body: 'body',
      author: 'me',
      created_utc: Date.now(),
      score: 1
    }]);

    await batchUpsertComments([{
      id: 'test_comment_1',
      body: 'hello',
      author: 'me',
      created_utc: Date.now(),
      post_id: 'test_post_1',
      parent_comment_id: null,
      score: 0
    }]);

    const p = await pool.query('SELECT * FROM posts WHERE id=$1', ['test_post_1']);
    const c = await pool.query('SELECT * FROM comments WHERE id=$1', ['test_comment_1']);
    console.log('post rows:', p.rows);
    console.log('comment rows:', c.rows);
  } catch (err) {
    console.error(err);
  } finally {
    pool.end();
  }
})();
