package trident.functions;

import java.util.Map;

import org.apache.log4j.Logger;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import trident.utils.Tools;
import trident.utils.TweetBuilder;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import entities.Tweet;

/**
 * Processes the tweet text to remove whitespaces, links and replies.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class TextProcessor extends BaseFunction {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 6642126263103950136L;

    /** The tb. */
    private TweetBuilder tb;

    /** The tools. */
    private Tools tools;

    /** The Constant LOG. */
    private static final Logger LOG = Logger.getLogger(TextProcessor.class);

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.BaseOperation#prepare(java.util.Map,
     * org.apache.storm.trident.operation.TridentOperationContext)
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        tools = new Tools();
        tb = new TweetBuilder((String) conf.get("PATH_TO_OOV_FILE"));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Function#execute(org.apache.storm.trident.tuple.TridentTuple,
     * org.apache.storm.trident.operation.TridentCollector)
     */
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Status s = null;
        String tweetText = null;
        try {
            s = TwitterObjectFactory.createStatus((String) tuple.getValue(0));
            tweetText = tools.removeLinksAndReplies(tb.removeSpacesInBetween(s.getText()));
        } catch (Exception e) {
            LOG.error(e.toString());
        }

        Tweet t = null;
        if (s != null) // rarely Twitter4J can't parse the json to convert to Status and Status is null.
            t = new Tweet(s.getId(), tweetText);
        else
            t = new Tweet(-1, " ");

        collector.emit(new Values(t));

    }

}
