/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package trident.utils;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import entities.DotProduct;
import entities.NearNeighbour;
import entities.SparseVector;
import entities.Tweet;

/**
 *
 * @author Mixos
 */
public class Tools implements Serializable{
    
    public Tools()
    {
        
    }

    /**
     * Reads from file and returns bufferedReader
     * @param inputFil
     * @return
     * @throws FileNotFoundException
     */
    public BufferedReader readFromFile(File inputFil) throws FileNotFoundException
    {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(inputFil)));
        return br;
    }
    

    /**
     * Retrieve the links starting with http:// and return the links along with the tweet
     * as final element after removing the links.
     * @param tweet
     * @return The tweet text without links and replies.
     */
    public String removeLinksAndReplies(String tweet)
    {
        while (tweet.contains("http://")) {
            tweet = removeSpecifiedWord(tweet, "http://");
        }
        
        while(tweet.contains("@")){
            tweet = removeSpecifiedWord(tweet, "@");
        }
        
        return tweet;
    }

    /**
     * Removes the word that starts with startChars from the tweet text.
     * @param tweet
     * @param startChars The starting characters of the word you want to remove
     * @return String A String without the words that start with startChars
     */
    private String removeSpecifiedWord(String tweet, String startChars)
    {
        String httpLink = "";
        int indexOfHttp = tweet.indexOf(startChars);
        int initialIndex = indexOfHttp;
        char currChar = tweet.charAt(indexOfHttp);
        httpLink += currChar;
        while ((indexOfHttp < tweet.length() - 1) && ((currChar != ' ') && (currChar != '\t'))) {
            indexOfHttp++;
            currChar = tweet.charAt(indexOfHttp);
            httpLink += currChar;
        }
        //remove http link from tweet
        tweet = (tweet.substring(0, initialIndex) + " " + tweet.substring(indexOfHttp + 1)).trim();
        //if you want to keep the specified string uncomment the following line
        //httpLinksAndTweetAtLastIndex.add(httpLink.toLowerCase().trim());    //add httpLink to temporary ArrayList
        return tweet;
    }

    /**
     * To allow testing this is a version without futures.
     * Takes an int array of positions and values and returns the int to be used as hash in buckets.
     * http://stackoverflow.com/questions/4844342/change-bits-value-in-byte
     * @return The smallHash as int.
     */
    public int computeIntHashAllowsTest(ArrayList<DotProduct> dtList)
    {
        int hash = 0;

        for (DotProduct dt : dtList)
            if (dt.getValue()>=0)
                hash = hash | (1<< dt.getPosition());

        return hash;
    }

        /**
     * Takes an int array of positions and values and returns the int to be used as hash in buckets.
     * http://stackoverflow.com/questions/4844342/change-bits-value-in-byte
     * @return The smallHash as int.
     */
    public int computeIntHash(ArrayList<Future<DotProduct>> dtList)
    {
        int hash = 0;
        try{
        for (Future<DotProduct> dt : dtList)
            if (dt.get().getValue()>=0)
                hash = hash | (1<< dt.get().getPosition());
        }
        catch (InterruptedException ex) {
                System.out.println(ex);
            }
            catch (ExecutionException ex) {
                System.out.println(ex);
            }

        return hash;
    }

    /* Constructs the reduced dimension hash from threads that give the position of each bit.
     * since reduced dimensions hash is e.g. 10001000, then int value should be 2^3 + 2^7
     * use int as hash representations. maximum k size is therefore 32 as contains(key)
     * can only use not more than ints.
     * @param list The list which contains the dot products derived from each thread
     * @return The complete hash
     */
    public int constructHashWithPow(ArrayList<Future<DotProduct>> list)
    {
        int sum = 0;
        for (Future<DotProduct> future : list) {
            try {
                if (future.get().getValue() >= 0) {
                    sum += Math.pow(2, future.get().getPosition());
                }
            }
            catch (InterruptedException ex) {
                System.out.println(ex);
            }
            catch (ExecutionException ex) {
                System.out.println(ex);
            }
        }
        return sum;
    }
    
        /**
     * Computes the cosine similarity between a possible Neighbour (possibly smaller dimension vector) and a new
     * Tweet that arrives which might have a bigger vector.
     * @param possibleNeighbour The old tweet, usually located in bucket.
     * @param newTweet The new arriving tweet.
     * @return NearNeighbour The cosine similarity along with the possible neighbour
     */
    public NearNeighbour computeCosineSimilarity(Tweet possibleNeighbour, Tweet newTweet) {
        SparseVector possibleNeighbourVect = possibleNeighbour.getSparseVector();
        SparseVector newTweetVect = newTweet.getSparseVector();
        IntArrayList nonZeroIndeces = new IntArrayList(possibleNeighbourVect.cardinality());
        DoubleArrayList dblZeroIndeces = new DoubleArrayList(possibleNeighbourVect.cardinality());
        possibleNeighbourVect.getNonZeros(nonZeroIndeces, dblZeroIndeces);
        double dotProductValue = newTweetVect.zDotProduct(possibleNeighbourVect, 0, possibleNeighbourVect.size(), nonZeroIndeces);

        //colt norm2 needs sqrt
        //here divide by zero will give NaN BUG if vectors are consisted ONLY of 0s !
        Double cosSim = new Double(dotProductValue / getNorm2(possibleNeighbourVect, newTweetVect));
        if (cosSim.isNaN()) {
            System.exit(1);
        }

        NearNeighbour nearN = new NearNeighbour(cosSim, possibleNeighbour);
        return nearN;

    }

    private double getNorm2(SparseVector possibleNeighbourVect, SparseVector newTweetVect) {
        return possibleNeighbourVect.getEuclidNorm() * newTweetVect.getEuclidNorm();
    }


    

}
