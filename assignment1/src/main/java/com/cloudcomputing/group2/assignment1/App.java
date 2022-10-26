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

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line
            .toLowerCase()
            .trim()
            .split("[ ,.;:?!“”()\\[\\]{}_-]"))
            .iterator());

        JavaRDD<String> validWords = words.filter(w -> w.matches("[a-z]+"));

        JavaPairRDD<String, Integer> wordOnes = validWords.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> wordCounts = wordOnes.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> wordOutput = wordCounts
            .mapToPair(x -> x.swap())
            .sortByKey(false)
            .mapToPair(x -> x.swap())
            .collect();

        JavaPairRDD<String, Integer> letterOnes = words.flatMap(w -> Arrays.asList(w.split("(?!^)")).iterator())
        .filter(w -> w.matches("[a-z]"))
        .mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> letterCounts = letterOnes.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> letterOutput = letterCounts.collect();

        for (Tuple2<?,?> tuple : wordOutput) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        for (Tuple2<?,?> tuple : letterOutput) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        spark.stop();
    }
}
