package trident.functions;

import java.util.List;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import entities.Tweet;

/**
 * The Class ExpandList.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class ExpandList extends BaseFunction {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 8381363446973738733L;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Function#execute(org.apache.storm.trident.tuple.TridentTuple,
     * org.apache.storm.trident.operation.TridentCollector)
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        List<Tweet> l = (List<Tweet>) tuple.getValue(0);
        if (l != null) {
            for (Tweet o : l) {
                collector.emit(new Values(o, o.getID()));
            }
        }
    }

}
