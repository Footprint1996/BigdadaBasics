package rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.actors.threadpool.Arrays;

public class LearnRDD {

    public static void main(String[] args) {
        JavaSparkContext sc =
                new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark RDD"));
        JavaRDD<String> lines = sc.textFile("E:\\spark\\word.txt");
        
        //Map
        JavaRDD<Iterable<String>> mapRdd = lines.map(new Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split);
            }
        });
        System.out.println(mapRdd.first());

        //FlatMap
        JavaRDD<String> flatMapRdd = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split);
            }
        });
        System.out.println(flatMapRdd.first());

        sc.close();
    }

}
