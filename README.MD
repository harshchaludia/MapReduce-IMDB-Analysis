# Map Reduce IMDB Analysis

In this project, we implemented a Map/Reduce program to find the number of movies with genre combinations of Comedy, Romance; Action, Thriller; and Adventure, Sci-Fi for the time periods
[2001-2005], [2006-2010], and [2011-2015] using IMDB dataset. In the second part of the project, we write SQL queries to find the top 5
and bottom 5 movies of the one of the time periods and all the genre combinations. We also find the query plan using the ‘EXPLAIN PLAN’ command.

## Getting Started

Below instructions will get you a version of the project up and running on your local machine for development and testing purposes. See the following, for notes on how to deploy the project on a live system.

### Prerequisites

1) Ubuntu (Version 14.04 or greater) or any linux platforms
2) Hadoop 2.9.1 or greater
3) Atom (File editor) 
4) Java 8 SE Development Toolkit 

### Procedure

• Install Virtual Box & download the ubuntu vdi image.
• Proceed with the installation of hadoop single node cluster.
• After completing the installation, g
• Start the hadoop daemons by typing the below command, and this starts all three nodes viz. namenode, datanode and secondary namenode.
  i.e start-dfs.sh start-yarn.sh
• Hadoop uses HDFS file system. Hence, we first had to decode the file system of Hadoop.
• This is how we load input,
  o Hadoop -dfs copyFromLocal imdb.txt
• To execute the project, perform the following operation.
  o bin/hadoop com.sun.tools.javac.main imdb.java
  o jar cf imdb.jar imdb*.class
  o hadoop jar project/imdb.jar project.imdb /imdb/input /imdb/output
 

## Running the tests

After successfull run of your hadoop program, go for downloading the output at "localhost:50070" in the browser.
