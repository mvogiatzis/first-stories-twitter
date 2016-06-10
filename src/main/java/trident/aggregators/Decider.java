package trident.aggregators;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import entities.NearNeighbour;
import entities.Tweet;

/**
 * Decides whether the closest tweet comes from bucket or most recently seen tweets and emits the result.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class Decider implements CombinerAggregator<NearNeighbour> {

    private static final long serialVersionUID = -6405441711557503789L;

    static class State {
        NearNeighbour closestNeighbor = null;
        Tweet t;
        int score = -1;

        public State(NearNeighbour closestNeighbor, Tweet t) {
            this.closestNeighbor = closestNeighbor;
            this.t = t;
        }
    }

    @Override
    public NearNeighbour init(TridentTuple tuple) {
        Tweet tw = (Tweet) tuple.getValueByField("tweet_obj");
        Tweet bucketTweet = (Tweet) tuple.getValueByField("coltweet_obj");
        double cosineBuckets = tuple.getDoubleByField("cosSimBckts");
        Object closestRecentObj = tuple.getValueByField("nnRecentTweet");
        NearNeighbour closestRecent = null;
        if (closestRecentObj != null)
            closestRecent = (NearNeighbour) closestRecentObj;

        if (closestRecent != null) {
            if (cosineBuckets >= closestRecent.getCosine())
                return new NearNeighbour(cosineBuckets, bucketTweet);
            else
                return closestRecent;
        } else {
            return new NearNeighbour(cosineBuckets, bucketTweet);
        }
    }

    @Override
    public NearNeighbour combine(NearNeighbour n1, NearNeighbour n2) {
        // if near neighbour is null, the tweet found in buckets is close enough
        if (n1.getCosine() >= n2.getCosine())
            return n1;

        return n2;
    }

    @Override
    public NearNeighbour zero() {
        return new NearNeighbour(-1.0, new Tweet(-1L));
    }

}
