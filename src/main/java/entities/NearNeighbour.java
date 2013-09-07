/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package entities;

import java.io.Serializable;

/**
 *
 * @author Mixos
 */
public class NearNeighbour implements Serializable{
    private double cosineSim;
    private Tweet nearestTweet;
    //ena apta 2 constructors poulo
    public NearNeighbour(double cos, Tweet nearestTweet)
    {
        this.cosineSim = cos;
        this.nearestTweet = nearestTweet;
    }
    
    public NearNeighbour()
    {
   
    }

    public void setCosineSim(double cos)
    {
        cosineSim = cos;
    }

    public void setTweet(Tweet neighb)
    {
        nearestTweet = neighb;
    }

    public Tweet getTweet()
    {
        return nearestTweet;
    }

    public double getCosine()
    {
        return cosineSim;
    }
}
