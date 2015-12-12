package com.gpjpe.domain;

/**
 * Created by Philipp on 12/12/15.
 */
public class HashtagCount {
    private String hashtag;
    private Long count;

    public HashtagCount(String hashtag, Long count) {
        this.hashtag = hashtag;
        this.count = count;
    }

    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag;
    }


    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }
}
