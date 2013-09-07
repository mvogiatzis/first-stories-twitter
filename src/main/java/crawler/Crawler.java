package crawler;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterBase;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 * Receives tweets from Twitter streaming API.
 * You need to pass the right parameters in the config.properties file.
 *
 */
public class Crawler {
	private static final Logger log = Logger.getLogger(Crawler.class);

	public static void main(String[] args) throws TwitterException, IOException {
		File file = new File("tweets.txt");
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		final FileWriter fw = new FileWriter(file.getAbsoluteFile());
		final BufferedWriter bw = new BufferedWriter(fw);

		StatusListener listener = new StatusListener() {
			int tweetCount = 0;

			public void onStatus(Status status) {
				String lang = status.getUser().getLang();
				if (tweetCount < 50000) {
					if (lang.equals("en")) {
						storeInFile(status);
						tweetCount++;
					}
				} else {
					
					try {
						bw.flush();
						bw.close();
						fw.close();
					} catch (IOException e) {
						log.error(e.toString());
					}
					System.exit(0);
				}

			}

			private void storeInFile(Status status) {
				try {
					bw.write(DataObjectFactory.getRawJSON(status));
					bw.newLine();
				} catch (Exception e) {
					log.error(e.toString());
				}
			}

			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			public void onException(Exception ex) {
				log.error(ex.toString());
			}

			public void onScrubGeo(long userId, long upToStatusId) {
			}

			public void onStallWarning(StallWarning warning) {
			}
		};
		// twitter stream authentication setup
		Properties prop = new Properties();
		try {
			InputStream in = Crawler.class.getClassLoader()
					.getResourceAsStream("twitter4j.properties");
			prop.load(in);
		} catch (IOException e) {
			log.error(e.toString());
		}
		//set the configuration
		ConfigurationBuilder twitterConf = new ConfigurationBuilder();
		twitterConf.setIncludeEntitiesEnabled(true);
		twitterConf.setDebugEnabled(Boolean.valueOf(prop.getProperty("debug")));
		twitterConf.setOAuthAccessToken(prop.getProperty("oauth.accessToken"));
		twitterConf.setOAuthAccessTokenSecret(prop
				.getProperty("oauth.accessTokenSecret"));
		twitterConf.setOAuthConsumerKey(prop.getProperty("oauth.consumerKey"));
		twitterConf.setOAuthConsumerSecret(prop
				.getProperty("oauth.consumerSecret"));
		twitterConf.setJSONStoreEnabled(true);

		TwitterStream twitterStream = new TwitterStreamFactory(
				twitterConf.build()).getInstance();
		twitterStream.addListener(listener);

		twitterStream.sample();
	}

}
