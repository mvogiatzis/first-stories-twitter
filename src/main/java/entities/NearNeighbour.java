package entities;

import java.io.Serializable;

/**
 * The Class NearNeighbour.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class NearNeighbour implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = 1675250272250930182L;

    /** The cosine sim. */
    private double cosineSim;

    /** The nearest tweet. */
    private Tweet nearestTweet;

    /**
     * Instantiates a new near neighbour.
     *
     * @param cos
     *            the cos
     * @param nearestTweet
     *            the nearest tweet
     */
    // ena apta 2 constructors poulo
    public NearNeighbour(double cos, Tweet nearestTweet) {
        this.cosineSim = cos;
        this.nearestTweet = nearestTweet;
    }

    /**
     * Instantiates a new near neighbour.
     */
    public NearNeighbour() {

    }

    /**
     * Sets the cosine sim.
     *
     * @param cos
     *            the new cosine sim
     */
    public void setCosineSim(double cos) {
        cosineSim = cos;
    }

    /**
     * Sets the tweet.
     *
     * @param neighb
     *            the new tweet
     */
    public void setTweet(Tweet neighb) {
        nearestTweet = neighb;
    }

    /**
     * Gets the tweet.
     *
     * @return the tweet
     */
    public Tweet getTweet() {
        return nearestTweet;
    }

    /**
     * Gets the cosine.
     *
     * @return the cosine
     */
    public double getCosine() {
        return cosineSim;
    }
}
