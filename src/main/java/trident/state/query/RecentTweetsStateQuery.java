package trident.state.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.QueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import trident.state.RecentTweetsDB;

import entities.NearNeighbour;
import entities.Tweet;

/**
 * Keeps the N most recent tweets and compares the tweet in question if the cosine similarity from buckets is lower than a given threshold.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class RecentTweetsStateQuery implements QueryFunction<RecentTweetsDB, NearNeighbour> {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 4227776035832647442L;

    /** The myturn. */
    private int partitionNum, numPartitions, myturn;

    /** The threshold. */
    private double threshold;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Operation#prepare(java.util.Map, org.apache.storm.trident.operation.TridentOperationContext)
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        partitionNum = context.getPartitionIndex();
        numPartitions = context.numPartitions();
        myturn = partitionNum;
        threshold = Double.valueOf((String) conf.get("THRESHOLD"));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.state.QueryFunction#batchRetrieve(org.apache.storm.trident.state.State, java.util.List)
     */
    @Override
    public List<NearNeighbour> batchRetrieve(RecentTweetsDB state, List<TridentTuple> args) {
        List<NearNeighbour> tweets = new ArrayList<NearNeighbour>();
        for (TridentTuple tuple : args) {
            Tweet tw = (Tweet) tuple.getValueByField("tweet_obj");
            double cosSimBuckets = tuple.getDoubleByField("cosSimBckts");

            NearNeighbour closestNeighbour = null;
            // check if comparisons are needed and iterate over all recent
            // tweets and find the closest
            if (cosSimBuckets <= threshold)
                closestNeighbour = state.getClosestNeighbour(tw);
            // else a close enough tweet has been found in the buckets

            tweets.add(closestNeighbour);

            // not every tweet should be inserted into the most recent tweets of each state
            // Insert in all partitions in a round-robin fashion
            // we sacrifice fault-tolerance here. If a partition fails and recovers,
            // two partitions may insert at the same time
            myturn++;
            if (myturn == numPartitions) {
                state.insert(tw);
                myturn = 0;
            }
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
    public void execute(TridentTuple tuple, NearNeighbour closestNeighbor, TridentCollector collector) {
        collector.emit(new Values(closestNeighbor));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Operation#cleanup()
     */
    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

}
