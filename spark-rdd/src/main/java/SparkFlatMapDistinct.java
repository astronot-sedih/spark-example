import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkFlatMapDistinct {
    public static void main(String args[]) {
            // configure spark
            SparkConf sparkConf = new SparkConf().setAppName("Spark Read FlatMap and Distinct")
                    .setMaster("local[2]").set("spark.executor.memory","1g");
            // start a spark context
            JavaSparkContext sc = new JavaSparkContext(sparkConf);

            // provide path to input text file
            String path = "../sample-data/sample-flatmap-text.txt";

            // read text file to RDD
            JavaRDD<String> lines = sc.textFile(path);

            // flatMap each line to words in the line
            JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).distinct();

            // collect RDD for printing
            for(String word:words.collect()){
                System.out.println(word);
        }
    }
}
