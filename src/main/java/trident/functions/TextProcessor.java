package trident.functions;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.log4j.Logger;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import trident.utils.Tools;
import trident.utils.TweetBuilder;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;
import backtype.storm.tuple.Values;
import entities.Tweet;

/**
 * Processes the tweet text to remove whitespaces, links and replies.
 *
 */
public class TextProcessor extends BaseFunction{
	
	private TweetBuilder tb;
	private Tools tools;
	private static final Logger LOG = Logger.getLogger(TextProcessor.class);
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		tools = new Tools();
        tb = new TweetBuilder((String) conf.get("PATH_TO_OOV_FILE"));
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Status s = null;
		String tweetText = null;
		try {
			s = DataObjectFactory.createStatus((String) tuple.getValue(0));
			tweetText = tools.removeLinksAndReplies(tb.removeSpacesInBetween(s.getText()));
		} catch (Exception e) {
			LOG.error(e.toString());
		}

		Tweet t = null;
		if (s!=null)	//rarely Twitter4J can't parse the json to convert to Status and Status is null.
			t = new Tweet(s.getId(), tweetText);
		else
			t = new Tweet(-1, " ");
		
		collector.emit(new Values(t));
		
	}

}
