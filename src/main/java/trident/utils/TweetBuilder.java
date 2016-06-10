package trident.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.Serializable;
import java.util.HashMap;

import java.util.StringTokenizer;

import entities.Tweet;

/**
 * The Class TweetBuilder.
 * 
 * @author Michael Vogiatzis (michaelvogiatzis@gmail.com)
 * @author Quentin Le Sceller (q.lesceller@gmail.com)
 */
public class TweetBuilder implements Serializable {

    /** The Constant serialVersionUID. */
    private static final long serialVersionUID = -5942213131652314190L;

    /** The oov words. */
    private HashMap<String, String> oovWords;

    /**
     * Instantiates a new tweet builder.
     */
    public TweetBuilder() {
        constructOOVWords("oov.txt");
    }

    /**
     * Instantiates a new tweet builder.
     *
     * @param pathToOOVFile
     *            the path to oov file
     */
    public TweetBuilder(String pathToOOVFile) {
        constructOOVWords(pathToOOVFile);
    }

    /**
     * Construct oov words.
     *
     * @param pathToOOVFile
     *            the path to oov file
     */
    void constructOOVWords(String pathToOOVFile) {
        oovWords = new HashMap<String, String>(575);
        fillOOVHashMap(new File(pathToOOVFile));
    }

    /**
     * Removes any whitespaces from the tweet body and returns the tweet text back.
     *
     * @param tweetBody
     *            the tweet body
     * @return the string
     */
    public String removeSpacesInBetween(String tweetBody) {
        StringBuilder body = new StringBuilder("");
        // now group all whitespaces as a delimiter
        // http://stackoverflow.com/questions/225337/how-do-i-split-a-string-with-any-whitespace-chars-as-delimiters
        for (String strToAppend : tweetBody.split("\\s+")) {
            body.append(strToAppend.concat(" "));
        }

        return body.toString().trim();
    }

    /**
     * Creates a tweet with id, timestamp and tweet body from the specified string line.
     *
     * @param strLine
     *            the str line
     * @return the tweet
     */
    public Tweet createTweetFromLine(String strLine) {
        StringTokenizer st = new StringTokenizer(strLine, "\t");
        Integer id = Integer.valueOf(st.nextToken());
        Long timestamp = Long.valueOf(st.nextToken());
        st.nextToken();

        return new Tweet(id, st.nextToken());
    }

    /**
     * Creates a tweet with id from the specified string line.
     *
     * @param strLine
     *            the str line
     * @return the tweet
     */
    public Tweet createTweetOnlyIDFromLine(String strLine) {
        StringTokenizer st = new StringTokenizer(strLine, "\t");
        // return id
        return new Tweet(Integer.valueOf(st.nextToken()));
    }

    /**
     * Gets tweet ID from given line. Tweet id should be the first token
     *
     * @param strLine
     *            the str line
     * @return The tweet ID.
     * @param: The
     *             line to get the tweet ID from.
     */
    public Integer getTweetID(String strLine) {
        StringTokenizer st = new StringTokenizer(strLine, "\t");
        return Integer.valueOf(st.nextToken());
    }

    /**
     * Gets the text body of a tweet exactly as it is.
     * 
     * @param strLine
     *            The line which contains the tweet
     * @return Returns the tweet text body - third token.
     */
    public String getTextBody(String strLine) {
        StringTokenizer st = new StringTokenizer(strLine, "\t");
        st.nextToken(); // gets id
        st.nextToken(); // gets timestamp
        st.nextToken(); // gets user
        return st.nextToken(); // returns tweetbody
    }

    /**
     * Gets the OOV normal word.
     *
     * @param key
     *            the key
     * @return the OOV normal word
     */
    public String getOOVNormalWord(String key) {
        return oovWords.get(key);
    }

    /**
     * Fill oov hash map.
     *
     * @param f
     *            the f
     */
    private void fillOOVHashMap(File f) {
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
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    /**
     * Gets the OOV hash map size.
     *
     * @return the OOV hash map size
     */
    public int getOOVHashMapSize() {
        return oovWords.size();
    }
}
