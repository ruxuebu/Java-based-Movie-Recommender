#!/bin/sh
rm *.java
rm *.class
rm driver.jar
rm recommenderResult
cp src/Recommender_System_Hadoop/src/* ./
hadoop com.sun.tools.javac.Main Driver.java DataDividerByUser.java CoOccurenceMatrixGenetator.java Multiplication.java RecommenderListGenerator.java MovieRelation.java TopK_RecommenderGenerator.java
jar cf driver.jar *.class
hdfs dfs -rm -r output*
hadoop jar driver.jar Driver watchHistory output1 output2 output3 output4 /watchHistory/watchHistory.txt /movieTitles/movieTitles.txt recommenderResult 2
