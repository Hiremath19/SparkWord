import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

public class SparkWord1 {
    public static void main(String args[]) {
        SparkConf conf = new SparkConf().setMaster("yarn").setAppName("sparktraining");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> filRdd=sc.textFile("E:\\wordcount\\wc.txt");


        System.out.println(filRdd.collect());
        JavaRDD<String> splitFile= filRdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] words= s.split(" ");
                ArrayList<String> strList = new ArrayList<>();
                for (String word : words)
                {
                    strList.add(word);
                }
                return strList.iterator();

            }
        });

        System.out.println(splitFile.collect());


        System.out.println( splitFile.countByValue());

    }
}