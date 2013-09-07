package trident.functions;

import java.util.Map;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import trident.utils.Tools;
import backtype.storm.tuple.Values;
import entities.NearNeighbour;
import entities.Tweet;

/**
 * Computes the cosine similarity between two tweets and emits the value.
 *
 */
public class ComputeDistance implements Function{

//	 NearNeighbour closestNN;
	 Tools tools;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		tools = new Tools();
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Tweet possibleNeighbour = (Tweet) tuple.getValueByField("coltweet_obj");
		Tweet newTweet = (Tweet) tuple.getValueByField("tweet_obj");
        NearNeighbour possibleClosest = tools.computeCosineSimilarity(possibleNeighbour, newTweet);
        
        collector.emit(new Values(possibleClosest.getCosine()));
	}

	@Override
	public void cleanup() {

	}

}
