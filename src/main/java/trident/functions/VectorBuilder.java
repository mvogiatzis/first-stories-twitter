package trident.functions;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import trident.utils.TweetBuilder;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;
import entities.SparseVector;
import entities.Tweet;

/**
 * This is responsible for converting the tweet object into a sparse vector based on previously seen terms.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 */
public class VectorBuilder implements Function {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -2995089719645563758L;

    /** The regex. */
    private final String regex = "[^A-Za-z0-9_£$%<>]";

    /** The tb. */
    private TweetBuilder tb;

    /** The position in pos map. */
    private static int positionInPosMap = 0;

    /** The uniq words. */
    private static HashMap<String, Integer> uniqWords;

    /** The position map. */
    private static HashMap<String, Integer> positionMap;

    /** The idf map. */
    private static HashMap<String, Double> idfMap;

    /** The number of previous tweets. */
    private static double numberOfPreviousTweets = 1.0;

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.storm.trident.operation.Operation#prepare(java.util.Map, org.apache.storm.trident.operation.TridentOperationContext)
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        int hashMapCapacity = Integer.valueOf((String) conf.get("UNIQUE_WORDS_EXPECTED"));
        uniqWords = new HashMap<String, Integer>(hashMapCapacity);
        positionMap = new HashMap<String, Integer>(hashMapCapacity);
        idfMap = new HashMap<String, Double>(hashMapCapacity);

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
        Tweet tweet = (Tweet) tuple.getValue(0);
        String tweetBody = tweet.getBody();

        String words[] = tweetBody.toLowerCase().split(regex);
        if (words.length > 0) {
            collector.emit(getValues(tweet, words));
        } else {
            tweetBody = "ONLYLINKSANDMENTIONZ";
            String dummyWord[] = { tweetBody };
            collector.emit(getValues(tweet, dummyWord));
        }
    }

    /**
     * Normalization by dividing with Euclid norm.
     *
     * @param vector
     *            the vector
     * @return the sparse vector
     */
    public SparseVector normalizeVector(SparseVector vector) {
        // NORMALIZE HERE with norm1 so a unit vector is produced
        IntArrayList indexes = new IntArrayList(vector.cardinality());
        DoubleArrayList dbls = new DoubleArrayList(vector.cardinality());
        double norm = vector.getEuclidNorm();
        vector.getNonZeros(indexes, dbls);
        for (int i = 0; i < indexes.size(); i++) {
            vector.setQuick(indexes.get(i), dbls.getQuick(i) / norm);
        }
        return vector;
    }

    /**
     * Gets the values.
     *
     * @param tweet
     *            the tweet
     * @param words
     *            the words
     * @return the values
     */
    private Values getValues(Tweet tweet, String[] words) {
        int dimensionsBefore = uniqWords.size();

        HashMap<String, Integer> tfMap = new HashMap<String, Integer>();
        // COMPUTE TF
        // COUNT TF
        String temp;
        Integer count;
        int uniqWordsIncrease;
        for (String w : words) {
            temp = tb.getOOVNormalWord(w);
            if (temp != null) {
                w = temp;
            }

            count = tfMap.get(w);

            if (count == null) {
                // Word first time in this document
                tfMap.put(w, 1);
                Integer countInOtherDocs = uniqWords.get(w);
                if (countInOtherDocs == null) {
                    // word first time in corpus
                    uniqWords.put(w, 1);
                    positionMap.put(w, positionInPosMap);
                    positionInPosMap++;
                } else {
                    // word first time in this document, but has been seen in corpus
                    uniqWords.put(w, countInOtherDocs + 1);
                }
            } else {
                // word has been seen in this document before
                tfMap.put(w, count + 1);
            }

        }

        numberOfPreviousTweets++;

        uniqWordsIncrease = uniqWords.size() - dimensionsBefore;

        SparseVector vector = new SparseVector(uniqWords.size());

        // now compute the vector using TF IDF weighting
        Double cnt;
        String pairKey;
        for (Map.Entry<String, Integer> pair : tfMap.entrySet()) {
            pairKey = pair.getKey();
            cnt = idfMap.get(pairKey);

            if (cnt == null) {
                cnt = Math.log10(numberOfPreviousTweets);
                // update idf
                idfMap.put(pairKey, Math.log10(numberOfPreviousTweets));
            } else {
                idfMap.put(pairKey, Math.log10((numberOfPreviousTweets) / (uniqWords.get(pairKey) + 1)));
            }

            vector.set(positionMap.get(pairKey), pair.getValue() * cnt);
        }

        vector.trimToSize(); // very precious line, saves in performance

        vector = normalizeVector(vector);
        vector.trimToSize();

        // idfs have been updated when constructing the vector

        tweet.setSparseVector(vector);
        return (new Values(tweet, uniqWordsIncrease));
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
