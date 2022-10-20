package com.cloudcomputing.group2.assignment1;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class App {
    public static void main(String[] args) {
        // CREATE SPARK CONTEXT
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[3]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        // LOAD DATASETS
        JavaRDD<String> text = sparkContext.textFile("./sample_files/sample-a.txt");

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
    }
}
