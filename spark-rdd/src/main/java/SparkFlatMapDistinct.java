import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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

            // list of even numbers
            List<Integer> even = Arrays.asList( 2, 4,5,5,6,7, 6, 8, 10);

            // list of odd numbers
            List<Integer> odd = Arrays.asList( 3, 5, 7, 9, 11);

            // list of prime numbers
            List<Integer> primes = Arrays.asList(17, 19, 23, 29, 31);


            // list of numbers
            List<List<Integer>> listOfNumbers = new ArrayList<>();
            listOfNumbers.add(even);
            listOfNumbers.add(odd);
            listOfNumbers.add(primes);

            System.out.println("Array of Integer Array : "+listOfNumbers);

            List<Integer> flattenedList = listOfNumbers
                    .stream() // read value inside array
                    .flatMap(l -> l.stream()) // flattening array
                    .distinct() // Remove duplicate value
                    .sorted() // sort desc
                    .collect(Collectors.toList()); // collect as one flattened array
            System.out.println("Processed Array of Integer Array : "+flattenedList);

    }
}
