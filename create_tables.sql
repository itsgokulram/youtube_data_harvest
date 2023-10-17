CREATE TABLE channels(
    channel_name VARCHAR(255),
    channel_id VARCHAR(255) UNIQUE,
    subscribers INT,
    views INT,
    description TEXT,
    total_videos INT,
    playlist_id VARCHAR(255)
);

CREATE TABLE playlists(
    playlist_id VARCHAR(255) UNIQUE,
    channel_id VARCHAR(255)
);

CREATE TABLE videos(
    video_id VARCHAR(255) UNIQUE,
    playlist_id VARCHAR(255),
    video_name VARCHAR(255),
    video_description TEXT,
    video_statistics VARCHAR(255),
    comment_count INT,
    view_count INT,
    like_count INT,
    favorite_Count INT,
    published_At VARCHAR(255),
    duration VARCHAR(255),
    thumbnail VARCHAR(255),
    caption_status VARCHAR(255)
);

CREATE TABLE comments(
    comment_id VARCHAR(255) UNIQUE,
    video_id VARCHAR(255),
    comment_text TEXT,
    comment_author VARCHAR(255),
    comment_published_date VARCHAR(255)
);
