package trident.state;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import storm.trident.state.State;
import cern.colt.list.IntArrayList;
import cern.colt.matrix.impl.DenseDoubleMatrix1D;
import entities.Bucket;
import entities.SparseVector;
import entities.Tweet;

/**
 * Holds a list of buckets and a list of random vectors.
 *
 */
public class BucketsDB implements State, Serializable{
	
	ArrayList<DenseDoubleMatrix1D[]> bucketRandVectors;
	List<Bucket> bucketList;
	
	private int partialL=0, k=0, queueSize=0;
	Random r;
	
	public BucketsDB(int partialL, int k, int queueSize){
		this.partialL = partialL;
		this.k = k;
		this.queueSize = queueSize;
		
		 r = new Random();
		//initialize Random Vectors
        bucketRandVectors = new ArrayList<DenseDoubleMatrix1D[]>();
        //in case not serializable
        int inputDims = 0;

        //compute the random vectors for each bucket
        for (int bckts = 0; bckts < partialL; bckts++) {
            DenseDoubleMatrix1D[] randVectList = new DenseDoubleMatrix1D[k];
            for (int dim = 0; dim < k; dim++) {
                randVectList[dim] = createRandomVector(inputDims);
            }
            bucketRandVectors.add(randVectList);
        }

        //initialize Buckets
        bucketList = new ArrayList<Bucket>(partialL);

        for (int bckt = 0; bckt < partialL; bckt++) {
            bucketList.add(new Bucket(queueSize, (int) Math.pow(2, k)));
        }
      
		
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
	 * Iterates over the bucket list and returns all near neighbours that share the same hash as
	 * the input tweet. 
	 * 
	 * @param tw The input tweet
	 * @return A list of colliding tweets out of all buckets.
	 */
	public ArrayList<Tweet> getPossibleNeighbors(Tweet tw){
		ArrayList<Tweet> possibleNeighbours = new ArrayList<Tweet>();
		int rBcktCounter = 0;
		
		for (Bucket bck : bucketList) {
            SparseVector sp = tw.getSparseVector();

            int smallHash = 0;
            for (int i = 0; i < k; i++) {

                DenseDoubleMatrix1D randomV = bucketRandVectors.get(rBcktCounter)[i];
                IntArrayList nonZeroIndeces = new IntArrayList(sp.cardinality());
                sp.getNonZeros(nonZeroIndeces, null);
                double dotProductValue = randomV.zDotProduct(sp, 0, sp.size(), nonZeroIndeces);
                if (dotProductValue >= 0) {
                    smallHash = smallHash | (1 << i);
                }

            }
            rBcktCounter++;
            //its partial possible neighbours because its per bucket colliding neighbors
                ArrayList<Tweet> partialPossibleNeighbours = findPossibleNeighbours(smallHash, bck);
                if (!partialPossibleNeighbours.isEmpty())
                    possibleNeighbours.addAll(partialPossibleNeighbours);

            //insert the tweet into the right bucket. no prob to insert it since the possible neighbours have
            //already been stored in possibleNeigh hashmap
            bck.insertIntoBucket(smallHash, tw);

        }//end of buckets
		
		return possibleNeighbours;
	}
	
	 /**
     * Increase the size of the random vectors by a given number of dimensions
     * 
     * @param byNumberOfDims The number of dimensions to increase.
     */
    public void updateRandomVectors(int byNumberOfDims) {

        if (byNumberOfDims <= 0) {
            return;
        }

       
        for (int bckt = 0; bckt < partialL; bckt++) {
            for (int i = 0; i < k; i++) {
                DenseDoubleMatrix1D oldVect = bucketRandVectors.get(bckt)[i];
                DenseDoubleMatrix1D biggerVect = new DenseDoubleMatrix1D(oldVect.size() + byNumberOfDims);

                //copy each value of old Vect to new bigger one
                int w;
                for (w = 0; w < oldVect.size(); w++) {
                    biggerVect.set(w, oldVect.get(w));
                }
                //for the remaining dimensions put gaussian values
                //since w==oldVect.size
                for (int index = 0; index < byNumberOfDims; index++) {
                    biggerVect.set(w + index, r.nextGaussian());
                }

                bucketRandVectors.get(bckt)[i] = biggerVect;
            }
        }
    }
    
    /**
     * Returns the list of tweets with the same hash as the input tweet.
     * 
     * @param smallHash The tweet's hash
     * @param bck The bucket to look into
     * @return A List of tweets with the same hash - possible neighbours.
     */
    private ArrayList<Tweet> findPossibleNeighbours(Integer smallHash, Bucket bck) {
        ArrayList<Tweet> possibleNeighbors = new ArrayList<Tweet>(); //helps not to null pointer exception
        ArrayList<Tweet> temp = bck.getCollidingTweets(smallHash);
        if (temp != null) {
            possibleNeighbors.addAll(temp);
        }

        return possibleNeighbors;
    }
	
    /**
     * Create random vector of given dimensions.  using norm1 vector.
     * @param dimensions
     * @return A normal unit vector
     */
    private DenseDoubleMatrix1D createRandomVector(int dims) {
        DenseDoubleMatrix1D randomVect = new DenseDoubleMatrix1D(dims);
//        Random r = new Random();
        double norm = 0, g;
        int i;
        for (i = 0; i < dims; i++) {
            g = r.nextGaussian();
            randomVect.setQuick(i, g);
            norm += Math.abs(g);
        }

        for (i = 0; i < dims; i++) {
            randomVect.setQuick(i, (randomVect.getQuick(i) / norm));
        }

        return randomVect;
    }
    
    public ArrayList<DenseDoubleMatrix1D[]> getRandomVectors(){
    	return bucketRandVectors;
    }
    

}
