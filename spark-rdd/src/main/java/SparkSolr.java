import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucidworks.spark.rdd.SolrJavaRDD;
import org.apache.solr.common.SolrDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkSolr {
    static String zkHost = "localhost:2181";
    static String collection = "twitter-index";
    static String solrQuery = "select?indent=true&q.op=OR&q=*%3A*&rows=100&start=0";

    public static void main(String[] args) {
        ObjectMapper om = new ObjectMapper();
        // configure spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("Spark Solr")
                .setMaster("local[2]").set("spark.executor.memory", "1g")
                .set("spark.master","local")
                .set("spark.driver.allowMultipleContexts","true")
                .set("spark.driver.only.wan","false");

        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SolrJavaRDD solrRDD = SolrJavaRDD.get(zkHost, collection, sc.sc());
        JavaRDD<SolrDocument> resultsRDD = solrRDD.queryShards(solrQuery);

        // READ FROM SOLR
        JavaRDD<String> datas = resultsRDD.map(om::writeValueAsString);
        datas.foreach(x -> {
            System.out.println(om.writeValueAsString(x));
        });

    }
}
