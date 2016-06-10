package trident.functions;

import java.util.Map;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import trident.utils.Tools;
import entities.NearNeighbour;
import entities.Tweet;

/**
 * Computes the cosine similarity between two tweets and emits the value.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class ComputeDistance implements Function {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 2954162920838873593L;

    /** The tools. */
    Tools tools;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Operation#prepare(java.util.Map, org.apache.storm.trident.operation.TridentOperationContext)
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        tools = new Tools();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Function#execute(org.apache.storm.trident.tuple.TridentTuple,
     * org.apache.storm.trident.operation.TridentCollector)
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Tweet possibleNeighbour = (Tweet) tuple.getValueByField("coltweet_obj");
        Tweet newTweet = (Tweet) tuple.getValueByField("tweet_obj");
        NearNeighbour possibleClosest = tools.computeCosineSimilarity(possibleNeighbour, newTweet);

        collector.emit(new Values(possibleClosest.getCosine()));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Operation#cleanup()
     */
    @Override
    public void cleanup() {

    }

}
