// db/batchUpsert.js
// ======================================================================
// This module contains two async functions that take arrays of scraped
// posts or comments and insert them into the PostgreSQL database in
// batches.  If a row with the same "id" already exists, it is updated
// instead of creating a duplicate (UPSERT behavior).
// ======================================================================

const pool = require('./connection.js'); // PostgreSQL connection pool

// ----------------------------------------------------------------------
// 1. Batch upsert POSTS
// ----------------------------------------------------------------------
/**
 * Inserts or updates many posts efficiently in chunks.
 * @param {Array<Object>} posts - array of post objects to insert/update
 * @param {number} chunkSize - how many posts to insert per SQL query
 */
async function batchUpsertPosts(posts, chunkSize = 50) {
  // Skip if posts is not an array or is empty
  if (!Array.isArray(posts) || posts.length === 0) return;

  // --- Helper: split a large array into smaller sub-arrays (chunks) ---
  const chunkArray = (arr, size) => {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
  };

  // Break posts into manageable groups (for very large datasets)
  const chunks = chunkArray(posts, chunkSize);

  // --- Process each chunk separately ---
  for (const chunk of chunks) {
    const colsPerRow = 7; // number of columns we insert per post
    const values = [];    // holds the actual data for parameter placeholders

    // Build a string of placeholder groups like:
    // ($1,$2,$3,$4,$5,$6,$7),($8,$9,$10,$11,$12,$13,$14)
    const placeholders = chunk
      .map((post, rowIndex) => {
        const offset = rowIndex * colsPerRow; // shift placeholder numbers per row

        // Push each field’s value into the values array
        values.push(
          post.id,
          post.subreddit,
          post.title,
          post.body,
          post.author,
          post.created_utc,
          post.score
        );

        // Create an array like ['$1','$2',...'$7'] for this row
        const nums = Array.from({ length: colsPerRow }, (_, i) => `$${offset + i + 1}`);
        return `(${nums.join(',')})`; // join into "( $1,$2,$3, ... )"
      })
      .join(','); // join all rows with commas

    // --- Final SQL statement with ON CONFLICT upsert logic ---
    const upsertPostDataQuery = `
      INSERT INTO posts (id, subreddit, title, body, author, created_utc, score)
      VALUES ${placeholders}
      ON CONFLICT (id) DO UPDATE
      SET subreddit = EXCLUDED.subreddit,
          title = EXCLUDED.title,
          body = EXCLUDED.body,
          author = EXCLUDED.author,
          created_utc = EXCLUDED.created_utc,
          score = EXCLUDED.score;
    `;

    try {
      // Execute the query, passing "values" as the data array
      await pool.query(upsertPostDataQuery, values);
    } catch (err) {
      console.error('❌ Batch insert posts error:', err);
      throw err; // rethrow so caller can handle it
    }
  }
}

// ----------------------------------------------------------------------
// 2. Batch upsert COMMENTS
// ----------------------------------------------------------------------
/**
 * Inserts or updates many comments efficiently in chunks.
 * @param {Array<Object>} comments - array of comment objects to insert/update
 * @param {number} chunkSize - how many comments per SQL query
 */
async function batchUpsertComments(comments, chunkSize = 200) {
  if (!Array.isArray(comments) || comments.length === 0) return;

  // Re-use the same chunking helper
  const chunkArray = (arr, size) => {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
  };

  const chunks = chunkArray(comments, chunkSize);

  for (const chunk of chunks) {
    const colsPerRow = 6; // id, body, author, created_utc, post_id, parent_comment_id
    const values = [];

    // Build placeholders and collect all values
    const placeholders = chunk
      .map((comment, rowIndex) => {
        const offset = rowIndex * colsPerRow;

        // Handle different field names that may come from the scraper
        const parentCommentId = comment.parent_comment_id ?? comment.parent_id ?? null;

        values.push(
          comment.id,
          comment.body,
          comment.author,
          comment.created_utc,
          parentCommentId,
          comment.post_id,
          comment.score
        );

        const nums = Array.from({ length: colsPerRow }, (_, i) => `$${offset + i + 1}`);
        return `(${nums.join(',')})`;
      })
      .join(',');

    // SQL statement: insert many rows or update if conflict on id
    const upsertCommentQuery = `
      INSERT INTO comments (id, body, author, created_utc, post_id, parent_comment_id)
      VALUES ${placeholders}
      ON CONFLICT (id) DO UPDATE
      SET body = EXCLUDED.body,
          author = EXCLUDED.author,
          created_utc = EXCLUDED.created_utc,
          post_id = EXCLUDED.post_id,
          parent_comment_id = EXCLUDED.parent_comment_id;
    `;

    try {
      await pool.query(upsertCommentQuery, values);
    } catch (err) {
      console.error('❌ Batch insert comments error:', err);
      throw err;
    }
  }
}

// ----------------------------------------------------------------------
// Export both functions so the scraper or processor can call them.
// ----------------------------------------------------------------------
module.exports = {
  batchUpsertPosts,
  batchUpsertComments
};
