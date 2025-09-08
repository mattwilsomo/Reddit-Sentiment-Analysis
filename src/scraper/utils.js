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



module.exports = { walkAndCollect };