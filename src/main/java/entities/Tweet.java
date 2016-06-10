package entities;

import java.io.Serializable;

/**
 * The Class Tweet.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 */
public class Tweet implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -5323477461801836101L;

    /** The id. */
    private long ID;

    /** The body. */
    private String body;

    /** The vector. */
    private SparseVector vector;

    /**
     * Instantiates a new tweet.
     *
     * @param tweetID
     *            the tweet id
     * @param body
     *            the body
     */
    public Tweet(long tweetID, String body) {
        this.body = body;

        this.vector = null;
        ID = tweetID;
    }

    /**
     * Instantiates a new tweet.
     *
     * @param tweetId
     *            the tweet id
     */
    public Tweet(long tweetId) {
        this.vector = null;
        ID = tweetId;
    }

    /**
     * Sets the sparse vector.
     *
     * @param sparseV
     *            the new sparse vector
     */
    public void setSparseVector(SparseVector sparseV) {
        this.vector = sparseV;
    }

    /**
     * Gets the body.
     *
     * @return the body
     */
    public String getBody() {
        return body;
    }

    /**
     * Sets the body.
     *
     * @param body
     *            the new body
     */
    public void setBody(String body) {
        this.body = body;
    }

    /**
     * Gets the sparse vector.
     *
     * @return the sparse vector
     */
    public SparseVector getSparseVector() {
        return vector;
    }

    /**
     * Checks for empty vector.
     *
     * @return true, if successful
     */
    public boolean hasEmptyVector() {
        return vector == null;
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    public long getID() {
        return ID;
    }

    /**
     * Sets the id.
     *
     * @param tweetId
     *            the new id
     */
    public void setId(long tweetId) {
        ID = tweetId;
    }
}
