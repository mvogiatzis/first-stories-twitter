package trident.state;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.trident.state.State;
import trident.utils.Tools;
import entities.NearNeighbour;
import entities.Tweet;

/**
 * The Class RecentTweetsDB.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class RecentTweetsDB implements State, Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -6551213539306685402L;

    /** The recent tweets. */
    LinkedBlockingQueue<Tweet> recentTweets;

    /** The tools. */
    Tools tools;

    /**
     * Instantiates a new recent tweets db.
     *
     * @param capacity
     *            the capacity
     * @param numPartitions
     *            the num partitions
     */
    public RecentTweetsDB(int capacity, int numPartitions) {
        // each state will have up to capacity/numPartitions elements in its queue
        recentTweets = new LinkedBlockingQueue<Tweet>(capacity / numPartitions);
        tools = new Tools();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.state.State#beginCommit(java.lang.Long)
     */
    @Override
    public void beginCommit(Long txid) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.state.State#commit(java.lang.Long)
     */
    @Override
    public void commit(Long txid) {
        // TODO Auto-generated method stub

    }

    /**
     * Gets the closest neighbour to the input tweet, by comparing all previously seen tweets that exist in the queue. Returns null if the
     * queue is empty
     * 
     * @param tweet
     *            Tweet in question
     * @return NearNeighbour The closest neighbour or null if the queue is empty.
     */
    public NearNeighbour getClosestNeighbour(Tweet tweet) {
        if (recentTweets.isEmpty())
            return null;

        Tweet firstTweet = recentTweets.peek();
        NearNeighbour closestNeighbor = new NearNeighbour(tools.computeCosineSimilarity(firstTweet, tweet).getCosine(), firstTweet);

        for (Tweet recentTweet : recentTweets) {
            NearNeighbour nnRecentTweet = tools.computeCosineSimilarity(recentTweet, tweet);
            if (nnRecentTweet.getCosine() > closestNeighbor.getCosine())
                closestNeighbor = nnRecentTweet;
        }

        return closestNeighbor;
    }

    /**
     * Insert.
     *
     * @param tw
     *            the tw
     */
    public void insert(Tweet tw) {
        // make space
        if (recentTweets.remainingCapacity() == 0)
            recentTweets.poll();

        recentTweets.offer(tw);
    }

}
