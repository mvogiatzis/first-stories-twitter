First Story Detection on Twitter using Storm
================================

*You can find a detailed description of the code logic at [this blogpost](http://micvog.com/2013/09/07/storm-first-story-detection/).*

This project tries to detect first stories (a.k.a new events) on Twitter as they happen.

Every day, thousands of posts share information about news, events, automatic updates (weather, songs) and personal information. The information published can be retrieved and analyzed in a news detection approach. The immediate spread of events on Twitter combined with the large number of Twitter users prove it suitable for first stories extraction. Towards this direction, this project deals with a distributed real-time first story detection (FSD) using Twitter on top of Storm. Specifically, I try to identify the first document in a stream of documents, which discusses about a specific event. Let’s have a look into the implementation of the methods used.

Implementation summary
------------------------------------
Upon a new tweet arrival, the tweet text is split into words and represented into the vector space. Then it is compared with the N tweets it is most likely to be similar to, using a method called [Locality Sensitive Hashing](http://en.wikipedia.org/wiki/Locality-sensitive_hashing). This data clustering method dramatically reduces the number of comparisons the tweet will need to find the N nearest neighbors and will be explained in detail later below. Having computed the distances with all near neighbor candidates, the tweet with the closest distance is assigned as the nearest. If the distance is above a certain threshold, the new tweet is considered a first story. A detailed version of the summary above will follow in the description of the bolts which act as the logical units of the application.

Documentation
----------------------
Algorithm's explanation and the code logic can be found on the [wiki](https://github.com/mvogiatzis/first-stories-twitter/wiki/_pages).

Storm
--------
Storm is a distributed real-time computation system which can guarantee data processing, high fault-tolerance and horizontal scaling to significantly large amounts of input data. It simplifies the implementation of parallel tasks by providing a programming interface suitable for stream processing and continuous computation. Having such a volume of input tweets streamed in real-time, FSD seemed like an ideal use case for Storm framework which can scale up the work by adding more resources.

**If Storm is new to you**  I highly recommend [this](https://github.com/nathanmarz/storm/wiki/Trident-tutorial) tutorial to familiarize yourself with the basic elements such as spouts, bolts, functions, groupings, aggregations and generally the streaming logic. The codebase depends heavily on them. However, the logic is the same and can be implemented outside Storm too.


How to run
---------------

This project uses Maven to build and run. Install Maven (preferably version 3.x) by following the [Maven installation instructions](http://maven.apache.org/download.cgi).

Tweets.txt is just a sample file, you should run the crawler first and gather a number of tweets to process. Alternatively you can connect a Twitter spout similar to [this](https://github.com/mvogiatzis/storm-unshortening/blob/master/src/main/java/spouts/TwitterSpout.java).

**To run the tweets crawler** type the following in the command line:
`mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=crawler.Crawler`

**To compile and run the project in local mode**, type the following command while being on the project root directory  
`mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=trident.FirstStoryDetection`

**To run in production cluster**, first package the code into a jar by running  
`mvn package`.  
This will package your code into a jar at the path `target/First-Story-Detection-{version}-jar-with-dependencies.jar.`  
Then you can submit your jar to the cluster using the `storm` client:  
`storm jar target/First-Story-Detection-1.0-SNAPSHOT-jar-with-dependencies.jar trident.FirstStoryDetection fsd`  

Please note that in the production cluster mode, you require a `DRPCClient` to feed the topology with tweets and get results.

Authors
======
* Michael Vogiatzis ([@mvogiatzis](https://twitter.com/mvogiatzis))

Contributing
------------------
Have you found any issues?  
Are there any features you want to improve (e.g. english crawler)? Even documentation or testing.
  
Please contact me at [michael@micvog.com](mailto:michael@micvog.com) or create a new Issue. Pull requests are always welcome. 

License
======
Copyright © 2013 Michael Vogiatzis  
See [License](https://github.com/mvogiatzis/first-stories-twitter/blob/master/LICENSE) for licensing information

