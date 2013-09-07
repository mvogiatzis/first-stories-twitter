package trident.functions;

import java.util.HashMap;
import java.util.Map;

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import trident.utils.TweetBuilder;
import backtype.storm.Config;
import backtype.storm.tuple.Values;
import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;
import entities.SparseVector;
import entities.Tweet;

/**
 * This is responsible for converting the tweet object into a sparse vector based on previously seen terms.
 *
 */
public class VectorBuilder implements Function{
    private final String regex = "[^A-Za-z0-9_£$%<>]";
    private TweetBuilder tb;
    private static int positionInPosMap = 0;
    private static HashMap<String, Integer> uniqWords;
    private static HashMap<String, Integer> positionMap;
    private static HashMap<String, Double> idfMap;
    private static double numberOfPreviousTweets = 1.0;
    
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		int hashMapCapacity=Integer.valueOf((String) conf.get("UNIQUE_WORDS_EXPECTED"));
    	uniqWords = new HashMap<String, Integer>(hashMapCapacity);
        positionMap = new HashMap<String, Integer>(hashMapCapacity);
        idfMap = new HashMap<String, Double>(hashMapCapacity);

        tb = new TweetBuilder((String) conf.get("PATH_TO_OOV_FILE"));
	}


	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Tweet tweet = (Tweet) tuple.getValue(0);
		String tweetBody = tweet.getBody();

        String words[] = tweetBody.toLowerCase().split(regex);
        if (words.length > 0) {
        	collector.emit(getValues(tweet, words));
        } else {
            tweetBody = "ONLYLINKSANDMENTIONZ";
            String dummyWord[] = {tweetBody};
            collector.emit(getValues(tweet, dummyWord));
        }
	}
	
	/**
     * Normalization by dividing with Euclid norm.
     * @param vector
     */
    public SparseVector normalizeVector(SparseVector vector) {
        //NORMALIZE HERE with norm1 so a unit vector is produced
        IntArrayList indexes = new IntArrayList(vector.cardinality());
        DoubleArrayList dbls = new DoubleArrayList(vector.cardinality());
        double norm = vector.getEuclidNorm();
        vector.getNonZeros(indexes, dbls);
        for (int i = 0; i < indexes.size(); i++) {
            vector.setQuick(indexes.get(i), dbls.getQuick(i) / norm);
        }
        return vector;
    }
    
    private Values getValues(Tweet tweet, String[] words){
        int dimensionsBefore = uniqWords.size();

            HashMap<String, Integer> tfMap = new HashMap<String, Integer>();
            //COMPUTE TF
            //COUNT TF
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
                    //Word first time in this document
                    tfMap.put(w, 1);
                    Integer countInOtherDocs = uniqWords.get(w);
                    if (countInOtherDocs == null) {
                        //word first time in corpus
                        uniqWords.put(w, 1);
                        positionMap.put(w, positionInPosMap);
                        positionInPosMap++;
                    } else {
                        //word first time in this document, but has been seen in corpus
                        uniqWords.put(w, countInOtherDocs + 1);
                    }
                } else {
                    //word has been seen in this document before
                    tfMap.put(w, count + 1);
                }

            }

            numberOfPreviousTweets++;

            uniqWordsIncrease = uniqWords.size() - dimensionsBefore;

            SparseVector vector = new SparseVector(uniqWords.size());
            
            //now compute the vector using TF IDF weighting
            Double cnt;
            String pairKey;
            for (Map.Entry<String, Integer> pair : tfMap.entrySet())
            {
                pairKey = pair.getKey();
                cnt = idfMap.get(pairKey);
                
                if (cnt == null){
                    cnt = Math.log10(numberOfPreviousTweets);
                    //update idf
                    idfMap.put(pairKey, Math.log10(numberOfPreviousTweets));
                }
                else{
                    idfMap.put(pairKey, Math.log10((numberOfPreviousTweets) / (uniqWords.get(pairKey) + 1)));
                }
                
                vector.set(positionMap.get(pairKey), pair.getValue() * cnt);
            }
            
            vector.trimToSize();    //very precious line, saves in performance

            vector = normalizeVector(vector);
            vector.trimToSize();

           //idfs have been updated when constructing the vector
            
            tweet.setSparseVector(vector);
            return (new Values(tweet, uniqWordsIncrease));  
    }
    
    
    @Override
	public void cleanup() {
		
	}


}
