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
public class Tweet implements Serializable{
    private long ID;
    private String body;
    private SparseVector vector;
    //private long timestamp;

    public Tweet(long tweetID, String body)
    {
        this.body = body;
        //this.timestamp = timestamp;
        this.vector = null;
        ID = tweetID;
    }
    
    public Tweet(long tweetId)
    {
        this.vector = null;
        ID = tweetId;
    }

//    public Tweet()
//    {
//
//    }


    public void setSparseVector(SparseVector sparseV)
    {
        this.vector = sparseV;
    }

    public String getBody()
    {
        return body;
    }
    
    public void setBody(String body){
        this.body = body;
    }
    
//    public long getTimeStamp()
//    {
//        return timestamp;
//    }

    public SparseVector getSparseVector()
    {
        return vector;
    }

    public boolean hasEmptyVector()
    {
        return vector == null;
    }

    public long getID()
    {
        return ID;
    }
    
    public void setId(long tweetId)
    {
        ID = tweetId;
    }
}
