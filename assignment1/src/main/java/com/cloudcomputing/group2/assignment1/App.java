package com.cloudcomputing.group2.assignment1;

import scala.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.FileWriter;
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

        // Words
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

        // Letters
        JavaPairRDD<String, Integer> letterOnes = words.flatMap(w -> Arrays.asList(w.split("(?!^)")).iterator())
                .filter(w -> w.matches("[a-z]"))
                .mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> letterCounts = letterOnes.reduceByKey((i1, i2) -> i1 + i2);

        // <Index, <Freq, Letter>>
        JavaPairRDD<Long, Tuple2<Integer, String>> sortedLettersWithIndex = letterCounts
                .sortByKey() // sorts into alphabetical order
                .mapToPair(x -> x.swap()) // <Freq, Letter>
                .sortByKey(false) // sorts by rank
                .zipWithIndex() // <<Freq, Letter>, Index>
                .mapToPair(x -> x.swap()); // <Index, <Freq, Letter>>

        long totalLetters = sortedLettersWithIndex.count();

        long popularUpperBoundL = (long) Math.ceil(totalLetters * 0.05); // this rounds up
        long rareLowerBoundL = (long) (totalLetters * 0.95);
        long commonLowerBoundL = (long) (totalLetters * 0.475);
        long commonUpperBoundL = (long) Math.ceil(totalLetters * 0.525);

        List<String> popularL = sortedLettersWithIndex
                .filterByRange(0L, popularUpperBoundL)
                .map(x -> String.join(", ",
                        List.of(Long.toString(x._1()), x._2()._2(), "popular", Integer.toString(x._2()._1()))))
                .collect();

        List<String> rareL = sortedLettersWithIndex
                .filterByRange(rareLowerBoundL, totalLetters)
                .map(x -> String.join(", ",
                        List.of(Long.toString(x._1()), x._2()._2(), "rare", Integer.toString(x._2()._1()))))
                .collect();

        List<String> commonL = sortedLettersWithIndex
                .filterByRange(commonLowerBoundL, commonUpperBoundL)
                .map(x -> String.join(", ",
                        List.of(Long.toString(x._1()), x._2()._2(), "common", Integer.toString(x._2()._1()))))
                .collect();

        // Output
        try {
            BufferedWriter w = new BufferedWriter(new FileWriter("/test-data/words_spark"));

            for (String s : popular) {
                System.out.println(s);
                w.write(s);
                w.newLine();
            }
            System.out.println();

            for (String s : common) {
                System.out.println(s);
                w.write(s);
                w.newLine();
            }
            System.out.println();

            for (String s : rare) {
                System.out.println(s);
                w.write(s);
                w.newLine();
            }
            w.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        try {
            BufferedWriter w = new BufferedWriter(new FileWriter("/test-data/letters_spark"));

            for (String s : popularL) {
                System.out.println(s);
                w.write(s);
                w.newLine();
            }
            System.out.println();

            for (String s : commonL) {
                System.out.println(s);
                w.write(s);
                w.newLine();
            }
            System.out.println();

            for (String s : rareL) {
                System.out.println(s);
                w.write(s);
                w.newLine();
            }
            w.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        spark.stop();
    }
}
