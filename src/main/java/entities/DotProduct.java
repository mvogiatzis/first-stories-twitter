/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package entities;

/**
 *
 * @author Mixos
 */
public class DotProduct {

    private double value;
    private int position;

    public DotProduct(double val, int pos)
    {
        value = val;
        position = pos;
    }

    public double getValue()
    {
        return value;
    }

    public int getPosition()
    {
        return position;
    }
    
}
