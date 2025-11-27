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

  const chunkArray = (arr, size) => {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
  };

  const chunks = chunkArray(comments, chunkSize);

  for (const chunk of chunks) {
    const colsPerRow = 7; // id, body, author, created_utc, parent_id, post_id, score
    const values = [];

    // Normalize each comment in the chunk and collect ids present in-chunk
    const normalized = chunk.map(c => {
      const rawId = c.id ?? c.name ?? null;
      const norm = (s) => {
        if (!s && s !== 0) return null;
        s = String(s);
        if (s.startsWith('t1_') || s.startsWith('t3_')) return s.slice(3);
        return s;
      };

      const parentRaw = c.parent_id ?? c.parent_comment_id ?? null;
      const postRaw = c.post_id ?? c.link_id ?? null;

      return {
        original: c,
        id: norm(rawId),
        parent_raw: parentRaw ? String(parentRaw) : null, // keep prefix for checks
        parent_norm: parentRaw ? norm(parentRaw) : null,
        post_norm: postRaw ? norm(postRaw) : null,
        body: c.body ?? null,
        author: c.author ?? (c.author && c.author.name) ?? null,
        created_utc: c.created_utc ?? null,
        score: c.score ?? null
      };
    });

    // Set of ids present in this chunk (strings)
    const chunkIds = new Set(normalized.map(x => x.id).filter(Boolean));

    // Collect candidate parent comment IDs (only for those that are comments, i.e. parent prefix t1_ or parent_norm looks like comment id)
    // We'll check DB for existence.
    const parentCandidates = [
      ...new Set(
        normalized
          .map(x => {
            // If parent_raw starts with t3_, it's a post, not a comment: skip parent candidacy.
            if (!x.parent_raw) return null;
            if (typeof x.parent_raw === 'string' && x.parent_raw.startsWith('t3_')) return null;
            // else parent_norm is candidate comment id
            return x.parent_norm;
          })
          .filter(Boolean)
      )
    ];

    // Query DB for parents that already exist (combine with chunkIds later)
    let existingParents = new Set();
    if (parentCandidates.length > 0) {
      try {
        const res = await pool.query(
          `SELECT id FROM comments WHERE id = ANY($1)`,
          [parentCandidates]
        );
        for (const r of res.rows) existingParents.add(String(r.id));
      } catch (err) {
        console.warn('Warning: parent-existence check failed — continuing and nulling unknown parents.', err);
        existingParents = new Set();
      }
    }

    // Merge chunkIds into existingParents (parents that exist in the same chunk are valid)
    for (const id of chunkIds) existingParents.add(id);

    // Build placeholders & values. IMPORTANT: ordering must match INSERT column order.
    const placeholders = normalized
      .map((c, rowIndex) => {
        const offset = rowIndex * colsPerRow;

        // Determine parent_id to insert:
        // - If parent_raw indicates a post (starts with t3_), we'll NOT insert it into parent_id (parent_id is for comments).
        // - If parent_norm exists and is present in existingParents set, use it.
        // - Otherwise null to avoid FK violation.
        let parentToInsert = null;
        if (c.parent_raw && typeof c.parent_raw === 'string' && c.parent_raw.startsWith('t3_')) {
          parentToInsert = null; // parent is a post (link stays in post_id)
        } else if (c.parent_norm && existingParents.has(String(c.parent_norm))) {
          parentToInsert = c.parent_norm;
        } else {
          parentToInsert = null;
        }

        // post_id goes to post column (we normalized it earlier)
        const postToInsert = c.post_norm ?? null;

        values.push(
          c.id ?? null,            // id
          c.body,                  // body
          c.author,                // author
          c.created_utc,           // created_utc
          parentToInsert,          // parent_id
          postToInsert,            // post_id
          c.score                  // score
        );

        const nums = Array.from({ length: colsPerRow }, (_, i) => `$${offset + i + 1}`);
        return `(${nums.join(',')})`;
      })
      .join(',');

    const upsertCommentQuery = `
      INSERT INTO comments (id, body, author, created_utc, parent_id, post_id, score)
      VALUES ${placeholders}
      ON CONFLICT (id) DO UPDATE
      SET body = EXCLUDED.body,
          author = EXCLUDED.author,
          created_utc = EXCLUDED.created_utc,
          parent_id = EXCLUDED.parent_id,
          post_id = EXCLUDED.post_id,
          score = EXCLUDED.score;
    `;

    // Run inside a transaction for safety
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query(upsertCommentQuery, values);
      await client.query('COMMIT');
    } catch (err) {
      await client.query('ROLLBACK').catch(()=>{});
      console.error('❌ Batch insert comments error:', err);
      client.release();
      throw err;
    } finally {
      try { client.release(); } catch(e) {}
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
