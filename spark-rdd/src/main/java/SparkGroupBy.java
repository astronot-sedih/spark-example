import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.SimpleDateFormat;
import java.util.*;

public class SparkGroupBy {

    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark CSV Group By Insert to Elastic")
                .setMaster("local[2]").set("spark.executor.memory", "1g")
                .set("spark.master","local")
                .set("es.index.auto.create","true")
                .set("es.nodes","localhost:9200")
                .set("spark.driver.allowMultipleContexts","true")
                .set("spark.driver.only.wan","false");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        ObjectMapper om = new ObjectMapper();
        // initialize an integer RDD
        JavaRDD<String> rdd_raw_data = sc.textFile("../sample-data/news_tweeters.csv");
        List<String> headers = Arrays.asList(rdd_raw_data.take(1).get(0).split(","));

        System.out.println("=============================================================");
        SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+00:00'");
        JavaPairRDD<Object, Iterable<Map<String, Object>>> rdd_data = rdd_raw_data.map(x -> {
            String[] test = x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            Map<String, Object> final_result = new HashMap<>();
            try {
                if (test[0].length() == 25) {
                    try {
                        Date d = df1.parse(test[0]);
                        long timeInMillis = d.getTime();
                        final_result.put("created_at",timeInMillis);
                        final_result.put("description",test[1]);
                        final_result.put("favouritesCount",Integer.parseInt(test[2]));
                        final_result.put("followersCount",Integer.parseInt(test[3]));
                        final_result.put("friendsCount",Integer.parseInt(test[4]));
                        final_result.put("location",test[5]);
                        final_result.put("mediaCount",Integer.parseInt(test[6]));
                        final_result.put("username",test[7]);
                        final_result.put("verified",Boolean.parseBoolean(test[8]));
                    } catch (Exception e) {
                        ;
                    }
                } else {
                    ;
                }
            } catch (Exception e) {;}
            return final_result;
        }).filter(x -> !(x.isEmpty())).groupBy(x -> x.get("verified"));
        System.out.println("=============================================================");
    }
}
