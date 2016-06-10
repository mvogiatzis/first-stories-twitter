package entities;

/**
 * The Class DotProduct.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class DotProduct {

    /** The value. */
    private double value;

    /** The position. */
    private int position;

    /**
     * Instantiates a new dot product.
     *
     * @param val
     *            the val
     * @param pos
     *            the pos
     */
    public DotProduct(double val, int pos) {
        value = val;
        position = pos;
    }

    /**
     * Gets the value.
     *
     * @return the value
     */
    public double getValue() {
        return value;
    }

    /**
     * Gets the position.
     *
     * @return the position
     */
    public int getPosition() {
        return position;
    }

}
