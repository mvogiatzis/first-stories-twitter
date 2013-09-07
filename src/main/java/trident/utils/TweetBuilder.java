/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package trident.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringTokenizer;

import entities.Tweet;

/**
 *
 */
public class TweetBuilder implements Serializable{

    private HashMap<String, String> oovWords;
    public TweetBuilder(){
    	constructOOVWords("oov.txt");
    }
    public TweetBuilder(String pathToOOVFile)
    {
        constructOOVWords(pathToOOVFile);
    }

    void constructOOVWords(String pathToOOVFile){
    	oovWords = new HashMap<String, String>(575);
        fillOOVHashMap(new File(pathToOOVFile));
    }
    
    /**
     * Removes any whitespaces from the tweet body and returns the tweet text back.
     * @param tweetBody
     * @return
     */
    public String removeSpacesInBetween(String tweetBody)
    {
        StringBuilder body = new StringBuilder("");
        //now group all whitespaces as a delimiter
        //http://stackoverflow.com/questions/225337/how-do-i-split-a-string-with-any-whitespace-chars-as-delimiters
        for (String strToAppend : tweetBody.split("\\s+")) {
            body.append(strToAppend.concat(" "));
        }

        return body.toString().trim();
    }

    /**
     * Creates a tweet with id, timestamp and tweet body from the specified string line.
     * @param strLine
     * @return
     */
    public Tweet createTweetFromLine(String strLine)
    {
        StringTokenizer st = new StringTokenizer(strLine, "\t");
        Integer id = Integer.valueOf(st.nextToken());
        Long timestamp = Long.valueOf(st.nextToken());
        st.nextToken();

        return new Tweet(id, st.nextToken());
    }
    
    /**
     * Creates a tweet with id from the specified string line.
     * @param strLine
     * @return
     */
    public Tweet createTweetOnlyIDFromLine(String strLine)
    {
        StringTokenizer st = new StringTokenizer(strLine, "\t");
        //return id
        return new Tweet(Integer.valueOf(st.nextToken()));
    }
    
    
    /**
     * Gets tweet ID from given line. Tweet id should be the first token
     * @param: The line to get the tweet ID from. 
     * @return The tweet ID.
     */
    public Integer getTweetID(String strLine)
    {
        StringTokenizer st = new StringTokenizer(strLine, "\t");
        return Integer.valueOf(st.nextToken());
    }

    /**
     * Gets the text body of a tweet exactly as it is.
     * @param strLine The line which contains the tweet
     * @return  Returns the tweet text body - third token.
     */
    public String getTextBody(String strLine)
    {
        StringTokenizer st = new StringTokenizer(strLine, "\t");
        st.nextToken(); //gets id
        st.nextToken(); //gets timestamp
        st.nextToken(); //gets user
        return st.nextToken(); //returns tweetbody
    }

    public String getOOVNormalWord(String key)
    {
        return oovWords.get(key);
    }

    private void fillOOVHashMap(File f)
    {
        Tools tools = new Tools();

        try {
            BufferedReader br = tools.readFromFile(f);
            String strLine = "";
            byte newLine = '\n';
            String[] words = new String[2];
            while ((strLine = br.readLine()) != null) {
                words = strLine.split("\t");
                oovWords.put(words[0], words[1]);
            }
            br.close();
        }
        catch (Exception e) {
            System.out.println(e);
        }

    }


    public int getOOVHashMapSize()
    {
        return oovWords.size();
    }
}
