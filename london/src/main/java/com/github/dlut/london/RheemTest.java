package com.github.dlut.london;

import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class RheemTest {

    public static void rheemTask(String pluginStr, String fileName) throws IOException {
        Date start = new Date();

        Plugin[] plugins = parsePlugins(pluginStr);
        Configuration configuration = new Configuration();
        LondonCase londonCase = new LondonCase(configuration, plugins);
        Collection<Tuple2<String, List<String>>> result = londonCase.execute(fileName);
        System.out.println(Collections.singletonList(result));

        Date end = new Date();
        System.out.println(fileName + " time: " + (end.getTime() - start.getTime()) + "ms");

//        printResult(result);
    }

    public static class LondonCase {
        private final Configuration configuration;

        private final Collection<Plugin> plugins;

        public LondonCase(Configuration configuration, Plugin... plugins) {
            this.configuration = configuration;
            this.plugins = Arrays.asList(plugins);
        }

        public Collection<Tuple2<String, List<String>>> execute(String inputUrl) {
            return this.execute(inputUrl, new ProbabilisticDoubleInterval(100, 10000, 0.8));
        }

        public Collection<Tuple2<String, List<String>>> execute(String inputUrl, ProbabilisticDoubleInterval pdi) {

            Map<String, String> categoryDic = new HashMap<>();
            categoryDic.put("Theft and Handling", "1");
            categoryDic.put("Violence Against the Person", "2");
            categoryDic.put("Criminal Damage", "3");
            categoryDic.put("Drugs", "4");
            categoryDic.put("Burglary", "5");
            categoryDic.put("Robbery", "6");
            categoryDic.put("Other Notifiable Offences", "7");
            categoryDic.put("Fraud or Forgery", "8");
            categoryDic.put("Sexual Offences", "9");

            RheemContext rheemContext = new RheemContext(this.configuration);
            this.plugins.forEach(rheemContext::register);

            return new JavaPlanBuilder(rheemContext)
                    .withJobName("London crime")
                    .withUdfJarOf(this.getClass())

                    .readTextFile(inputUrl).withName("Load file")

                    .map(line -> line.split(","))
//                    .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                    .withName("Map - split")

                    .map(record -> {
                        try {
                            if (!categoryDic.get(record[2]).isEmpty()) {
                                record[2] = categoryDic.get(record[2]);
                            } else {
                                record[2] = "0";
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return record;
                    })
//                    .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                    .withName("Map - dict")

                    .filter(record -> !record[4].equals("0") && !record[4].equals("1"))
//                    .withSelectivity(0.99, 0.99, 0.99)
                    .withName("Filter")

                    .sort(record -> new Integer(record[5]) * new Integer(record[6]))
//                    .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                    .withName("Sort")

                    .map(record -> {
                        List<String> result = new ArrayList<>(Arrays.asList(record));
                        result.add(record[4]);
                        return new Tuple2<>(String.valueOf((new Integer(record[6]) - 1) / 3 + 1), result);
//                        result.add(String.valueOf((new Integer(record[6]) - 1) / 3 + 1));
                    })
//                    .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                    .withName("Map - toList")

                    .reduceByKey(
                            Tuple2::getField0,
                            (record1, record2) -> {
                                List<String> result;
                                if (Integer.parseInt(record1.getField1().get(4)) >= Integer.parseInt(record2.getField1().get(4))) {
                                    result = record1.getField1();
                                } else {
                                    result = record2.getField1();
                                }
                                result.set(7, String.valueOf(Integer.parseInt(record1.getField1().get(7)) +
                                        Integer.parseInt(record2.getField1().get(7))));
                                return new Tuple2<>(record1.getField0(), result);
                    })
//                    .withCardinalityEstimator(new DefaultCardinalityEstimator(0.9, 1, false, in -> Math.round(0.01 * in[0])))
                    .withName("ReduceByKey")

                    .collect();
        }
    }

    private static Plugin[] parsePlugins(String pluginStr) {
        return Arrays.stream(pluginStr.split(","))
                .map(token -> {
                    switch (token) {
                        case "java":
                            return Java.basicPlugin();
                        case "spark":
                            return Spark.basicPlugin();
                        default:
                            throw new IllegalArgumentException("Unknown platform: " + token);
                    }
                })
                .collect(Collectors.toList()).toArray(new Plugin[0]);
    }

    private static void printResult(Collection<Tuple2<String, List<String>>> result) {
        System.out.printf("Found %d line:\n", result.size());
        int numPrintedWords = 0;
        for (Tuple2<String, List<String>> count : result) {
            if (++numPrintedWords >= 10) break;
            System.out.printf("%s\n", count.getField0());
            count.getField1().forEach(data -> {
                System.out.print(data + " ");
            });
            System.out.println("---");
        }
    }
}
