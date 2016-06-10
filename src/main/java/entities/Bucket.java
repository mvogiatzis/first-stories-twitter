package entities;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;

/**
 * The Class Bucket.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class Bucket implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 8492375240506657440L;

    /** The queue size. */
    private int queueSize;

    /** The hash table. */
    private HashMap<Integer, ArrayDeque<Tweet>> hashTable;

    /**
     * Instantiates a new bucket.
     *
     * @param queueSize
     *            the queue size
     * @param numOfMaxSmallHashes
     *            the num of max small hashes
     */
    public Bucket(int queueSize, int numOfMaxSmallHashes) {
        hashTable = new HashMap<Integer, ArrayDeque<Tweet>>(numOfMaxSmallHashes);
        this.queueSize = queueSize;
    }

    /**
     * Returns whether the hash is contained in the hashTable.
     *
     * @param hash
     *            the hash
     * @return True if it's present, false if not.
     */
    public boolean containsHash(int hash) {
        return hashTable.containsKey(hash);
    }

    /**
     * Gets the colliding tweets.
     *
     * @param hash
     *            the hash
     * @return the colliding tweets
     */
    public ArrayDeque<Tweet> getCollidingTweets(int hash) {
        return hashTable.get(hash);
    }

    /**
     * Insert the tweet into the specified big bucket by comparing with the smallHashes that the bucket contains.
     *
     * @param smallHashToAdd
     *            the small hash to add
     * @param tweet
     *            The tweet to get inserted
     */
    public void insertIntoBucket(Integer smallHashToAdd, Tweet tweet) {
        if (containsHash(smallHashToAdd)) {
            addToCollidingTweets(smallHashToAdd, tweet);
        } else {
            putNewPair(smallHashToAdd, tweet);
        }
    }

    /**
     * Adds the tweet into the right similar documents list.
     *
     * @param smallHash
     *            the small hash
     * @param tweet
     *            the tweet
     */
    private void addToCollidingTweets(int smallHash, Tweet tweet) {
        ArrayDeque<Tweet> tweetList = getCollidingTweets(smallHash);
        if (!isArrayListFull(tweetList)) {
            getCollidingTweets(smallHash).offer(tweet);
        } else {
            getCollidingTweets(smallHash).poll();
            getCollidingTweets(smallHash).offer(tweet);
        }
    }

    /**
     * Checks if is array list full.
     *
     * @param queue
     *            the queue
     * @return true, if is array list full
     */
    private boolean isArrayListFull(ArrayDeque<Tweet> queue) {
        return (queue.size() == queueSize);
    }

    /**
     * Put new pair.
     *
     * @param smallHashToAdd
     *            the small hash to add
     * @param tweet
     *            the tweet
     */
    private void putNewPair(int smallHashToAdd, Tweet tweet) {
        ArrayDeque<Tweet> arr = new ArrayDeque<Tweet>(queueSize);
        arr.add(tweet);
        hashTable.put(smallHashToAdd, arr);
    }

    /**
     * Gets the size.
     *
     * @return the size
     */
    public int getSize() {
        return hashTable.size();
    }

    /**
     * Gets the list of queues.
     *
     * @return the list of queues
     */
    public Collection<ArrayDeque<Tweet>> getListOfQueues() {
        return hashTable.values();
    }

}
