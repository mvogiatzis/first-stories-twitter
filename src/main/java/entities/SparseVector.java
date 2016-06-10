package entities;

import cern.colt.list.DoubleArrayList;
import cern.colt.matrix.impl.SparseDoubleMatrix1D;

/**
 * The Class SparseVector.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 */
public class SparseVector extends SparseDoubleMatrix1D {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -1449716566474695683L;

    /**
     * Instantiates a new sparse vector.
     *
     * @param size
     *            the size
     */
    public SparseVector(int size) {
        super(size);
    }

    /**
     * Instantiates a new sparse vector.
     *
     * @param values
     *            the values
     */
    public SparseVector(double[] values) {
        super(values);
    }

    /**
     * Returns the Euclid norm which is sum(x[i]^2).
     *
     * @return the euclid norm
     */
    public double getEuclidNorm() {
        DoubleArrayList dbls = new DoubleArrayList(this.cardinality());
        this.getNonZeros(null, dbls);
        double norm = 0;
        for (int i = 0; i < dbls.size(); i++) {
            norm += Math.pow(dbls.getQuick(i), 2);
        }
        return Math.sqrt(norm);
    }

}
