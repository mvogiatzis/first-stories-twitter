package trident.state.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;
import trident.state.RecentTweetsDB;
import backtype.storm.tuple.Values;
import entities.NearNeighbour;
import entities.Tweet;

/**
 * Keeps the N most recent tweets and compares the tweet in question if
 * the cosine similarity from buckets is lower than a given threshold.
 *
 */
public class RecentTweetsStateQuery implements
		QueryFunction<RecentTweetsDB, NearNeighbour> {

	private int partitionNum, numPartitions, myturn;
	private double threshold;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		partitionNum = context.getPartitionIndex();
		numPartitions = context.numPartitions();
		myturn = partitionNum;
		threshold = Double.valueOf((String) conf.get("THRESHOLD"));
	}

	@Override
	public List<NearNeighbour> batchRetrieve(RecentTweetsDB state,
			List<TridentTuple> args) {
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

			//not every tweet should be inserted into the most recent tweets of each state
			//Insert in all partitions in a round-robin fashion
			//we sacrifice fault-tolerance here. If a partition fails and recovers, 
			//two partitions may insert at the same time
			myturn++;
			if (myturn == numPartitions) {
				state.insert(tw);
				myturn = 0;
			}
		}

		return tweets;
	}

	@Override
	public void execute(TridentTuple tuple, NearNeighbour closestNeighbor,
			TridentCollector collector) {
		collector.emit(new Values(closestNeighbor));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

}
