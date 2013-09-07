package trident.functions;

import backtype.storm.tuple.Values;
import entities.NearNeighbour;
import entities.Tweet;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Extracts the information from the NearNeighbour object and emits them.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 *
 */
public class Extractor extends BaseFunction{

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		NearNeighbour nn = (NearNeighbour) tuple.getValueByField("nn");
		Long nnId = 0L;
		Double cosine = nn.getCosine();
		Tweet nnTweet = nn.getTweet();
		String nnText="Null text";		
		if (nnTweet!=null){
			nnId = nnTweet.getID();
			nnText = nnTweet.getBody();
			}
		
		collector.emit(new Values(nnId, nnText, cosine));
	}

}
