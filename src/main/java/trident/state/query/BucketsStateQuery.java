package trident.state.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import entities.Tweet;

import backtype.storm.tuple.Values;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.QueryFunction;
import storm.trident.tuple.TridentTuple;
import trident.state.BucketsDB;

/**
 * Holds the state for a number of buckets. Each bucket will return near neighbours
 * that their hash collide with the tweet in question.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 *
 */
public class BucketsStateQuery extends BaseQueryFunction<BucketsDB, ArrayList<Tweet>>{
	
	@Override
	public List<ArrayList<Tweet>> batchRetrieve(BucketsDB state,
			List<TridentTuple> args) {
		List<ArrayList<Tweet>> tweets = new ArrayList<ArrayList<Tweet>>();
		for(TridentTuple tuple : args){
			Tweet tw = (Tweet) tuple.getValue(0);
			int byNumberOfDims = tuple.getInteger(1);	//number of dimensions to upgrade
			state.updateRandomVectors(byNumberOfDims);
			ArrayList<Tweet> possibleNeighbors = state.getPossibleNeighbors(tw);
			tweets.add(possibleNeighbors);
		}
		
		return tweets;
	}

	@Override
	public void execute(TridentTuple tuple, ArrayList<Tweet> collidingTweets,
			TridentCollector collector) {
		//emit by tweet id
		Tweet tw = (Tweet) tuple.getValue(0);
		collector.emit(new Values(tw.getID(), collidingTweets));
	}


}
