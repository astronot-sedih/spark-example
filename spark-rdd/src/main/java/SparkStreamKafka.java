import java.text.SimpleDateFormat;
import java.util.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

public class SparkStreamKafka {

    public static <T> T getNestedValue(Map map, String... keys) {
        Object value = map;

        for (String key : keys) {
            value = ((Map) value).get(key);
        }

        return (T) value;
    }

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper om = new ObjectMapper();
        // SPARK CONIFG IS HERE
        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Stream Kafka and insert to Elastic")
                .setMaster("local[2]")
                .set("es.index.auto.create","true")
                .set("es.nodes","localhost:9200")
                .set("es.mapping.id","id")
                // PORT ELASTIC
                .set("spark.driver.allowMultipleContexts","true")
                .set("spark.driver.only.wan","false");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(60000));

        // LOGGER CONFIG IS HERE
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.OFF);

        // KAFKA CONIFG IS HERE
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:6667");
        // BROKER SERVER
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "kafkaConsumer2");
        // GROUP ID FOR SPARK JOB
        kafkaParams.put("auto.offset.reset", "latest");
        // KAFKA OFFSET
        kafkaParams.put("enable.auto.commit", false);


        // TOPIC SET
        Collection<String> topics = Arrays.asList("tw-post-v3");

        // CREATE DIRECT STREAM FROM SPARK TO KAFKA
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        // CREATING TUPLE PAIR
        stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));

        // GETTING DATA STREAM FROM KAFKA
        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() {
            @Override
            public String call(ConsumerRecord<String, String> v1) throws Exception {
                // RETURN IS JSON STRING
                return v1.value();
            }
        });

        // FOREACH RDD PER LINE
        lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> t) throws Exception {
                // GET RDD AS OBJECT PER LINE AND REMAPPING
                JavaRDD<Map<String, Object>> rdd_data = t.distinct().map(x -> {

                    Map<String,Object> final_map = new HashMap<>();
                    SimpleDateFormat df1 = new SimpleDateFormat("E MMM dd HH:mm:ss '+0000' yyyy");

                    Date d = df1.parse(om.readValue(x.trim(),Map.class).get("created_at").toString());
                    long timeInMillis = d.getTime();
                    final_map.put("id",om.readValue(x.trim(),Map.class).get("id"));
                    final_map.put("created_at",timeInMillis);
                    final_map.put("username",getNestedValue(om.readValue(x.trim(),Map.class),"user","name"));
                    final_map.put("location", getNestedValue(om.readValue(x.trim(), Map.class), "user", "location") == null || getNestedValue(om.readValue(x.trim(), Map.class), "user", "location") == "" ? "" : getNestedValue(om.readValue(x.trim(),Map.class),"user","location"));
                    final_map.put("favouritesCount",getNestedValue(om.readValue(x.trim(),Map.class),"user","favourites_count"));
                    final_map.put("friendsCount",getNestedValue(om.readValue(x.trim(),Map.class),"user","friends_count"));
                    final_map.put("followersCount",getNestedValue(om.readValue(x.trim(),Map.class),"user","followers_count"));
                    final_map.put("mediaCount",getNestedValue(om.readValue(x.trim(),Map.class),"user","statuses_count"));
                    final_map.put("verified",getNestedValue(om.readValue(x.trim(),Map.class),"user","verified"));
                    final_map.put("description",om.readValue(x.trim(),Map.class).get("text").toString().trim());
                    return final_map;
                }).filter(x -> !(x.isEmpty()));
                JavaEsSpark.saveToEs(rdd_data,"twitter-index/_doc");
//                FOR CHECK PRINTOUT VALUE IS HERE
//                System.out.println("=============================================");
//                rdd_data.foreach(x -> {
//                    System.out.println("final_map = " + x);
//                });
            }
        });
        jssc.start();
        jssc.awaitTermination();
    }
}