package trident.functions;

import entities.NearNeighbour;
import entities.Tweet;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Extracts the information from the NearNeighbour object and emits them.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class Extractor extends BaseFunction {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -9096570120969145885L;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Function#execute(org.apache.storm.trident.tuple.TridentTuple,
     * org.apache.storm.trident.operation.TridentCollector)
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        NearNeighbour nn = (NearNeighbour) tuple.getValueByField("nn");
        Long nnId = 0L;
        Double cosine = nn.getCosine();
        Tweet nnTweet = nn.getTweet();
        String nnText = "Null text";
        if (nnTweet != null) {
            nnId = nnTweet.getID();
            nnText = nnTweet.getBody();
        }

        collector.emit(new Values(nnId, nnText, cosine));
    }

}
