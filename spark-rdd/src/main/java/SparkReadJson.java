import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SparkReadJson {

    public static <T> T getNestedValue(Map map, String... keys) {
        Object value = map;

        for (String key : keys) {
            value = ((Map) value).get(key);
        }

        return (T) value;
    }

    public static void main(String[] args) {
        // configure spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Read JSON Insert Elastic")
                .setMaster("local[2]").set("spark.executor.memory", "1g")
                .set("spark.master", "local")
                .set("es.index.auto.create", "true")
                .set("es.nodes", "localhost:9200")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.driver.only.wan", "false");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        ObjectMapper om = new ObjectMapper();
        // initialize an integer RDD
        JavaRDD<String> rdd_raw_data = sc.textFile("../sample-data/news.json");
        JavaRDD<Object> rdd = rdd_raw_data.distinct().map(x -> x);

        // check size rdd
        System.out.println(rdd.collect().size());


//      Check per object rdd
        for(Object i: rdd.collect()){
            System.out.println(i);;
        }

//      Reprocess RDD to Map Class
        JavaRDD<Map<String, Object>> rdd_data = rdd_raw_data.distinct().map(x -> {
            Map<String, Object> final_map = new HashMap<>();
            SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'+00:00'");

            Date d = df1.parse(om.readValue(x.trim(), Map.class).get("date").toString());
            long timeInMillis = d.getTime();
            final_map.put("id", om.readValue(x.trim(), Map.class).get("id"));
            final_map.put("created_at",timeInMillis);
            final_map.put("username", getNestedValue(om.readValue(x.trim(), Map.class), "user", "username"));
            final_map.put("location", getNestedValue(om.readValue(x.trim(), Map.class), "user", "location") == null || getNestedValue(om.readValue(x.trim(), Map.class), "user", "location") == "" ? "" : getNestedValue(om.readValue(x.trim(),Map.class),"user","location"));
            final_map.put("favouritesCount", getNestedValue(om.readValue(x.trim(), Map.class), "user", "favouritesCount"));
            final_map.put("friendsCount", getNestedValue(om.readValue(x.trim(), Map.class), "user", "friendsCount"));
            final_map.put("followersCount", getNestedValue(om.readValue(x.trim(), Map.class), "user", "followersCount"));
            final_map.put("mediaCount", getNestedValue(om.readValue(x.trim(), Map.class), "user", "mediaCount"));
            final_map.put("verified", getNestedValue(om.readValue(x.trim(), Map.class), "user", "verified"));
            final_map.put("description", om.readValue(x.trim(), Map.class).get("content").toString().trim());
            return final_map;
        }).filter(x -> !(x.isEmpty()));

//      INSERT TO ELASTIC
        JavaEsSpark.saveToEs(rdd_data,"twitter-index/_doc");
    }
}
