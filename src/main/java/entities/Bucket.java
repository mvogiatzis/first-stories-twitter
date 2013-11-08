package entities;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class Bucket implements Serializable{

    private int queueSize;
    private HashMap<Integer, ArrayDeque<Tweet>> hashTable;

    public Bucket(int queueSize, int numOfMaxSmallHashes)
    {
        hashTable = new HashMap<Integer, ArrayDeque<Tweet>>(numOfMaxSmallHashes);
        this.queueSize = queueSize;
    }

    /**
     * Returns whether the hash is contained in the hashTable
     * @param hash
     * @return True if it's present, false if not.
     */
    public boolean containsHash(int hash)
    {
        return hashTable.containsKey(hash);
    }

    public ArrayDeque<Tweet> getCollidingTweets(int hash)
    {
        return hashTable.get(hash);
    }

    /**
     * Insert the tweet into the specified big bucket by comparing with the smallHashes that the bucket contains.
     * @param smallHash The tweet's hash
     * @param tweet The tweet to get inserted
     */
    public void insertIntoBucket(Integer smallHashToAdd, Tweet tweet)
    {
        if (containsHash(smallHashToAdd)) {
            addToCollidingTweets(smallHashToAdd, tweet);
        }
        else {
            putNewPair(smallHashToAdd, tweet);
        }
    }
    /**
     * Adds the tweet into the right similar documents list.
     * @param smallHash
     * @param tweet
     */
    private void addToCollidingTweets(int smallHash, Tweet tweet)
    {
    	ArrayDeque<Tweet> tweetList = getCollidingTweets(smallHash);
        if (!isArrayListFull(tweetList))
        {
            getCollidingTweets(smallHash).offer(tweet);
        }
        else
        {
            getCollidingTweets(smallHash).poll();
            getCollidingTweets(smallHash).offer(tweet);
        }
    }

    private boolean isArrayListFull(ArrayDeque<Tweet> queue)
    {
        return (queue.size()==queueSize);
    }

    private void putNewPair(int smallHashToAdd, Tweet tweet)
    {
    	ArrayDeque<Tweet> arr = new ArrayDeque<Tweet>(queueSize);
        arr.add(tweet);
        hashTable.put(smallHashToAdd, arr);
    }

    public int getSize()
    {
        return hashTable.size();
    }

    public Collection<ArrayDeque<Tweet>> getListOfQueues()
    {
        return hashTable.values();
    }



}
