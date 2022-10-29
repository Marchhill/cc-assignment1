package com.cloudcomputing.group2.assignment1;

import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import java.io.IOException;
import java.util.ArrayList;
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

        JavaPairRDD<String, Integer> wordCounts = validWords.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        // <Index, <Freq, Word>>
        JavaPairRDD<Long, Tuple2<Integer, String>> sortedWordsWithIndex = wordCounts
                .sortByKey() // sorts into alphabetical order
                .mapToPair(x -> x.swap()) // <Freq, Word>
                .sortByKey(false) // sorts by rank
                .zipWithIndex() // <<Freq, Word>, Index>
                .mapToPair(x -> x.swap()); // <Index, <Freq, Word>>

        long totalWords = sortedWordsWithIndex.count();

        long popularUpperBound = (long) Math.ceil(totalWords * 0.05); // this rounds up
        long rareLowerBound = (long) (totalWords * 0.95);
        long commonLowerBound = (long) (totalWords * 0.475);
        long commonUpperBound = (long) Math.ceil(totalWords * 0.525);

        List<String> popular = sortedWordsWithIndex
                .filterByRange(0L, popularUpperBound)
                .map(x -> String.join(", ",
                        List.of(Long.toString(x._1()), x._2()._2(), "popular", Integer.toString(x._2()._1()))))
                .collect();

        List<String> rare = sortedWordsWithIndex
                .filterByRange(rareLowerBound, totalWords)
                .map(x -> String.join(", ",
                        List.of(Long.toString(x._1()), x._2()._2(), "rare", Integer.toString(x._2()._1()))))
                .collect();

        List<String> common = sortedWordsWithIndex
                .filterByRange(commonLowerBound, commonUpperBound)
                .map(x -> String.join(", ",
                        List.of(Long.toString(x._1()), x._2()._2(), "common", Integer.toString(x._2()._1()))))
                .collect();

        // List<Tuple2<String, Integer>> sortedWords
        // .collect();

        JavaPairRDD<String, Integer> letterOnes = words.flatMap(w -> Arrays.asList(w.split("(?!^)")).iterator())
                .filter(w -> w.matches("[a-z]"))
                .mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> letterCounts = letterOnes.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> letterOutput = letterCounts.collect();

        for (String s : popular) {
            System.out.println(s);
        }
        System.out.println();
        for (String s : common) {
            System.out.println(s);
        }
        System.out.println();
        for (String s : rare) {
            System.out.println(s);
        }

        for (Tuple2<?, ?> tuple : letterOutput) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        spark.stop();
    }
}
