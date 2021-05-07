package com.github.dlut.wordcount.java;

import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.util.Arrays;
import java.util.Collection;

public class VideoWordCount {

    public static void main(String[] args) {

        String inputUrl = "file:/D:/2020project/rheem/essay.txt";
        //String inputUrl = "hdfs://10.141.221.217:9000/firstin/file1.txt";

        RheemContext rheemCtx = new RheemContext(new Configuration())
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemCtx)
                .withJobName(String.format("WordCount (%s)", inputUrl))
                .withUdfJarOf(VideoWordCount.class);

        Collection<Tuple2<String, Integer>> wordcounts = planBuilder

                .readTextFile(inputUrl).withName("Load file")

                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withSelectivity(10, 100, 0.9)
                .withName("Split words")

                // Filter empty tokens.
                .filter(token -> !token.isEmpty())
                .withSelectivity(0.99, 0.99, 0.99)
                .withName("Filter empty words")

                .map(word -> new Tuple2<>(word.toLowerCase(), 1))
                .withName("To lower case, add counter")

                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(),
                                t1.getField1() + t2.getField1()))
                .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                .withName("Add counters")

                .collect();

        System.out.println(wordcounts);
    }

}
