package trident.state;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;

import storm.trident.state.State;
import trident.utils.Tools;
import entities.NearNeighbour;
import entities.Tweet;

public class RecentTweetsDB implements State, Serializable {

	LinkedBlockingQueue<Tweet> recentTweets;
	Tools tools;

	public RecentTweetsDB(int capacity, int numPartitions) {
		//each state will have up to capacity/numPartitions elements in its queue
		recentTweets = new LinkedBlockingQueue<Tweet>(capacity/numPartitions);
		tools = new Tools();
	}

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub

	}

	/**
	 * Gets the closest neighbour to the input tweet, by comparing all
	 * previously seen tweets that exist in the queue. Returns null if the queue
	 * is empty
	 * 
	 * @param tweet
	 *            Tweet in question
	 * @return NearNeighbour The closest neighbour or null if the queue is
	 *         empty.
	 */
	public NearNeighbour getClosestNeighbour(Tweet tweet) {
		if (recentTweets.isEmpty())
			return null;

		Tweet firstTweet = recentTweets.peek();
		NearNeighbour closestNeighbor = new NearNeighbour(tools
				.computeCosineSimilarity(firstTweet, tweet).getCosine(),
				firstTweet);

		for (Tweet recentTweet : recentTweets) {
			NearNeighbour nnRecentTweet = tools.computeCosineSimilarity(
					recentTweet, tweet);
			if (nnRecentTweet.getCosine() > closestNeighbor.getCosine())
				closestNeighbor = nnRecentTweet;
		}

		return closestNeighbor;
	}

	public void insert(Tweet tw) {
		// make space
		if (recentTweets.remainingCapacity() == 0)
			recentTweets.poll();
		
		recentTweets.offer(tw);
	}

}
