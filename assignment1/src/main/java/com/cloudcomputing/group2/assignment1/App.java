package com.cloudcomputing.group2.assignment1;

import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class App {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: Group 2 wordcount <file>");
            System.exit(1);
        }
      
        SparkSession spark = SparkSession
            .builder()
            .appName("Group2WordCount")
            .getOrCreate();

        // CREATE SPARK CONTEXT
        //SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[3]");
        //JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap( title -> Arrays.asList(title
            .toLowerCase()
            .trim()
            .replaceAll("\\p{Punct}","")
            .split(" ")).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
        System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();

        /*

        // LOAD DATASETS
        JavaRDD<String> text = spark.read().textFile("./sample_files/sample-a.txt");

        // TRANSFORMATIONS
        JavaRDD<String> titles = text
                //.map(YoutubeTitleWordCount::extractTitle)
                .filter(StringUtils::isNotBlank);

        JavaRDD<String> words = titles.flatMap( title -> Arrays.asList(title
                .toLowerCase()
                .trim()
                .replaceAll("\\p{Punct}","")
                .split(" ")).iterator());
		
        // COUNTING
        Map<String, Long> wordCounts = words.countByValue();
        List<Map.Entry> sorted = wordCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue()).collect(Collectors.toList());
      
        // DISPLAY
        for (Map.Entry<String, Long> entry : sorted) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
        */
    }
}
