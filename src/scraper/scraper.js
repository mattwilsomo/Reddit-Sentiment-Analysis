require("dotenv").config();
const Snoowrap = require("snoowrap")

const r = new Snoowrap({
    userAgent: process.env.USER_AGENT,
    clientId: process.env.CLIENT_ID,
    clientSecret: process.env.CLIENT_SECRET,
    username: process.env.REDDIT_USER,
    password: process.env.REDDIT_PASS
})

async function fetchPosts() {
    const posts = await r.getSubreddit("technology").getHot().then(console.log)

}

fetchPosts()