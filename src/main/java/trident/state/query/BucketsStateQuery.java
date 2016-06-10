package trident.state.query;

import java.util.ArrayList;
import java.util.List;

import entities.Tweet;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import trident.state.BucketsDB;

/**
 * Holds the state for a number of buckets. Each bucket will return near neighbours that their hash collide with the tweet in question.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class BucketsStateQuery extends BaseQueryFunction<BucketsDB, ArrayList<Tweet>> {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 4144018750182205981L;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.state.QueryFunction#batchRetrieve(org.apache.storm.trident.state.State, java.util.List)
     */
    @Override
    public List<ArrayList<Tweet>> batchRetrieve(BucketsDB state, List<TridentTuple> args) {
        List<ArrayList<Tweet>> tweets = new ArrayList<ArrayList<Tweet>>();
        for (TridentTuple tuple : args) {
            Tweet tw = (Tweet) tuple.getValue(0);
            int byNumberOfDims = tuple.getInteger(1); // number of dimensions to upgrade
            state.updateRandomVectors(byNumberOfDims);
            ArrayList<Tweet> possibleNeighbors = state.getPossibleNeighbors(tw);
            tweets.add(possibleNeighbors);
        }

        return tweets;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.state.QueryFunction#execute(org.apache.storm.trident.tuple.TridentTuple, java.lang.Object,
     * org.apache.storm.trident.operation.TridentCollector)
     */
    @Override
    public void execute(TridentTuple tuple, ArrayList<Tweet> collidingTweets, TridentCollector collector) {
        // emit by tweet id
        Tweet tw = (Tweet) tuple.getValue(0);
        collector.emit(new Values(tw.getID(), collidingTweets));
    }

}
