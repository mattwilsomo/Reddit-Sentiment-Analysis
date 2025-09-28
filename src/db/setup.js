// import the database connection 
const pool = require('./connection.js');

// Function to set up the database tables
async function setupDatabase() {
    // Get a client from the pool
    const client = await pool.connect();

    try{
        // Create posts table
        const createPostsTableQuery = `
        CREATE TABLE IF NOT EXISTS posts(
            id VARCHAR(255) PRIMARY KEY,
            title TEXT,
            body TEXT,
            author TEXT,
            created_utc BIGINT
        );`
        
        const createCommentTableQuery = `
        CREATE TABLE IF NOT EXISTS comments(
            id VARCHAR(255) PRIMARY KEY,
            body TEXT,
            author TEXT,
            created_utc BIGINT,
            post_id VARCHAR(255) REFERENCES posts(id),
            parent_comment_id VARCHAR(255) REFERENCES comments(id)
        );`
        
        await client.query(createPostsTableQuery)
        console.log("Database table 'posts is ready'")
        await client.query(createCommentTableQuery)
        console.log("Database table 'comments' is ready")    

    } catch(e){
        console.error("Error setting up database table ", e)
    } finally {
        client.release()
    }

}