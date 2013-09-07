package trident;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Fields;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import trident.aggregators.CountAggKeep;
import trident.aggregators.Decider;
import trident.functions.ComputeDistance;
import trident.functions.ExpandList;
import trident.functions.Extractor;
import trident.functions.TextProcessor;
import trident.functions.VectorBuilder;
import trident.state.query.BucketsStateQuery;
import trident.state.query.RecentTweetsStateQuery;
import trident.state.BucketsDB;
import trident.state.RecentTweetsDB;
import trident.utils.FirstNAggregator;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

/**
 * Main class to run FSD
 */
public class FirstStoryDetection {

	public final static String TOPOLOGY_NAME = "fsd";
	private static final Logger LOG = Logger
			.getLogger(FirstStoryDetection.class);

	public static class BucketsStateFactory implements StateFactory {

		int partialL, k, queueSize;

		public BucketsStateFactory(int partialL, int k, int queueSize) {
			this.partialL = partialL;
			this.k = k;
			this.queueSize = queueSize;
		}

		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return new BucketsDB(partialL, k, queueSize);
		}

	}

	public static class RecentTweetsStateFactory implements StateFactory {
		@Override
		public State makeState(Map conf, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return new RecentTweetsDB(Integer.valueOf((String) conf
					.get("RECENT_TWEETS_TO_COMPARE_WITH")), numPartitions);
		}

	}

	public static StormTopology buildTopology(LocalDRPC drpc) {
		TridentTopology topology = new TridentTopology();
		Properties prop = new Properties();
		int partialL = 0, k = 0, queueSize = 0, bucketsParallelism = 0, L = 0, computeDistance = 0, recentTweetsParalellism = 0;
		try {
			// load a properties file
			FileInputStream finputstream = new FileInputStream(
					"config.properties");
			prop.load(finputstream);
			L = Integer.valueOf(prop.getProperty("L"));
			// partial L will give the number of buckets each thread (given by
			// parallelism hint) will hold
			partialL = L
					/ Integer.valueOf(prop.getProperty("BucketsParallelism"));
			k = Integer.valueOf(prop.getProperty("k"));
			queueSize = Integer.valueOf(prop.getProperty("QUEUE_SIZE"));
			bucketsParallelism = Integer.valueOf(prop
					.getProperty("BucketsParallelism"));
			computeDistance = Integer.valueOf(prop
					.getProperty("ComputeDistance"));
			recentTweetsParalellism = Integer.valueOf(prop
					.getProperty("RecentTweetsStateQuery"));
			finputstream.close();
		} catch (IOException ex) {
			System.err.println(ex);
		}

		TridentState bucketsDB = topology
				.newStaticState(new BucketsStateFactory(partialL, k, queueSize));
		TridentState recentTweetsDB = topology
				.newStaticState(new RecentTweetsStateFactory());

		// Comment out the debug rows to get debugging
		topology.newDRPCStream(TOPOLOGY_NAME, drpc)
				.each(new Fields("args"), new TextProcessor(),
						new Fields("textProcessed"))
				.each(new Fields("textProcessed"), new VectorBuilder(),
						new Fields("tweet_obj", "uniqWordsIncrease"))
				// .each(new Fields("tweet_obj", "uniqWordsIncrease"), new
				// Debug());
				.broadcast()
				.stateQuery(bucketsDB,
						new Fields("tweet_obj", "uniqWordsIncrease"),
						new BucketsStateQuery(),
						new Fields("tw_id", "collidingTweetsList"))
				.parallelismHint(bucketsParallelism)
				.each(new Fields("collidingTweetsList"), new ExpandList(),
						new Fields("coltweet_obj", "coltweetId"))
				.groupBy(new Fields("tw_id", "coltweetId"))
				.aggregate(
						new Fields("coltweetId", "tweet_obj", "coltweet_obj"),
						new CountAggKeep(),
						new Fields("count", "tweet_obj", "coltweet_obj"))
				// how many times each colliding tweet
				// is seen per tweet.
				// CountAggKeep keeps the fields we passed in the config map
				// (tweet_obj in our case)
				// .each(new Fields("tw_id", "coltweetId", "tweet_obj",
				// "count"),
				// new Debug());
				.groupBy(new Fields("tw_id"))
				.aggregate(
						new Fields("count", "coltweetId", "tweet_obj",
								"coltweet_obj"),
						new FirstNAggregator(3 * L, "count", true),
						new Fields("countAfter", "coltweetId", "tweet_obj",
								"coltweet_obj"))
				.each(new Fields("tw_id", "coltweetId", "tweet_obj",
						"coltweet_obj"), new ComputeDistance(),
						new Fields("cosSim"))
				.parallelismHint(computeDistance)
				// .each(new Fields("tw_id", "coltweetId", "cosSim"), new
				// Debug());
				.shuffle()
				.groupBy(new Fields("tw_id"))
				// find closest neighbor
				.aggregate(
						new Fields("coltweetId", "tweet_obj", "coltweet_obj",
								"cosSim"),
						new FirstNAggregator(1, "cosSim", true), // give only
																	// the
																	// closest
																	// neighbor
						new Fields("coltweetId", "tweet_obj", "coltweet_obj",
								"cosSimBckts"))
				// .each(new Fields("tw_id", "coltweetId", "cosSimBckts"), new
				// Debug());
				.broadcast()
				// tweet should go to all partitions to parallelize the task
				.stateQuery(recentTweetsDB,
						new Fields("tweet_obj", "cosSimBckts"),
						new RecentTweetsStateQuery(),
						new Fields("nnRecentTweet"))
				.parallelismHint(recentTweetsParalellism)
				// .each(new Fields("tw_id", "cosSimBckts", "nnRecentTweets"),
				// new Debug());
				.groupBy(new Fields("tw_id"))
				.aggregate(
						new Fields("coltweetId", "tweet_obj", "coltweet_obj",
								"cosSimBckts", "nnRecentTweet"), new Decider(),
						new Fields("nn"))
				.each(new Fields("nn"), new Extractor(),
						new Fields("colId", "col_txt", "cos"))
				.project(new Fields("colId", "col_txt", "cos"));
		// .each(new Fields("tw_id", "nn"), new Debug());

		return topology.build();
	}
	
	private static Config createTopologyConfiguration(Properties prop,
			boolean localMode) {
		Config conf = new Config();
		List<String> dprcServers = new ArrayList<String>();
		dprcServers.add("localhost");

		conf.put(Config.DRPC_SERVERS, dprcServers);
		conf.put(Config.DRPC_PORT, 3772);
		if (!localMode)
			conf.put(Config.STORM_CLUSTER_MODE, new String("distributed"));

		conf.put("UNIQUE_WORDS_EXPECTED",
				prop.getProperty("UNIQUE_WORDS_EXPECTED"));
		conf.put("PATH_TO_OOV_FILE", prop.getProperty("PATH_TO_OOV_FILE"));
		conf.put("L", prop.getProperty("L"));
		conf.put("BucketsParallelism", prop.getProperty("BucketsParallelism"));
		conf.put("k", prop.getProperty("k"));
		conf.put("QUEUE_SIZE", prop.getProperty("QUEUE_SIZE"));
		List<String> countAggKeepFields = new ArrayList<String>();
		countAggKeepFields.add("tweet_obj");
		countAggKeepFields.add("coltweet_obj");
		conf.put("countAggKeepFields", countAggKeepFields);
		conf.put("THRESHOLD", prop.getProperty("THRESHOLD"));
		conf.put("RECENT_TWEETS_TO_COMPARE_WITH",
				prop.getProperty("RECENT_TWEETS_TO_COMPARE_WITH"));
		conf.setDebug(false);

		conf.setNumWorkers(Integer.valueOf((String) prop
				.get("NUMBER_OF_WORKERS")));
		conf.setMaxSpoutPending(50000000);
		return conf;
	}

	public static void main(String[] args) throws Exception {
		Properties prop = new Properties();
		String queryFile = null;
		FileOutputStream fos = null;
		try {
			// load a properties file
			FileInputStream finputstream = new FileInputStream(
					"config.properties");
			prop.load(finputstream);
			queryFile = prop.getProperty("PATH_TO_QUERY_FILE");
			fos = new FileOutputStream(new File(
					prop.getProperty("PATH_TO_OUTPUT_FILE")));
			finputstream.close();
		} catch (IOException ex) {
			System.err.println(ex);
		}

		if (args == null || args.length == 0) {
			Config conf = createTopologyConfiguration(prop, true);
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology(TOPOLOGY_NAME, conf, buildTopology(drpc));

			Thread.sleep(2000); // give it some time to setup

			BufferedReader br = new BufferedReader(new FileReader(queryFile));
			String tweetJson;
			fos.write("Start: ".getBytes());
			fos.write(String.valueOf(System.currentTimeMillis()).getBytes());
			byte[] newLine = "\n".getBytes();
			int times = 0;
			// emit tweets into topology
			while ((tweetJson = br.readLine()) != null) {

				String result = drpc.execute(TOPOLOGY_NAME, tweetJson);

				Status s = null;
				try {
					s = DataObjectFactory.createStatus(tweetJson);
					result = s.getId() + "\t" + s.getText() + "\t" + result;
				} catch (TwitterException e) {
					LOG.error(e.toString());
				}

				fos.write(result.getBytes());
				fos.write(newLine);

				// times++;
				// if (times == 1000)
				// break;
			}
			fos.write(newLine);
			fos.write("Finish: ".getBytes());
			fos.write(String.valueOf(System.currentTimeMillis()).getBytes());

			fos.flush();
			fos.close();
			br.close();
			drpc.shutdown();
			cluster.shutdown();
		} else {
			// distributed mode
			Config conf = createTopologyConfiguration(prop, false);
			LocalDRPC drpc = null;
			StormSubmitter.submitTopology(args[0], conf, buildTopology(drpc));
		}

	}

}
