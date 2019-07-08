package mysparkkafkalikes.service;

import kafka.serializer.StringDecoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

@Service
@Slf4j
public class SparkLikeService implements Serializable {
    private static final long serialVersionUID = 3L;

    @Value("${like.topic}")
    private String topic;

    public void launch() {
        SparkConf conf = new SparkConf()
                .setAppName("mySparkLikes")
                .setMaster("local[*]")
                .set("spark.default.parallelism", "15")
                .set("spark.streaming.concurrentJobs", "5")
                .set("spark.executor.memory", "1G")
                .set("spark.cores.max", "3")
                .set("spark.local.dir", "/tmp/mySparkLikes")
                .set("spark.streaming.kafka.maxRatePerPartition", "5");

        Set<String> topics = Collections.singleton(topic);

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "127.0.0.1:9092");

        JavaStreamingContext jsc = new JavaStreamingContext(
                new JavaSparkContext(conf),
                Durations.seconds(3));
        jsc.checkpoint("checkpoint"); //保证元数据恢复，就是Driver端挂了之后数据仍然可以恢复

        // 得到数据流
        final JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );
        System.out.println("stream started!");
        stream.print();

        JavaPairDStream<Integer, Integer> countDStream = stream
                .transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<Integer, Integer>>() {

                    @Override
                    public JavaPairRDD<Integer, Integer> call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
                        return stringStringJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, String>, Integer, Integer>() {

                            @Override
                            public Tuple2<Integer, Integer> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                                return new Tuple2<>(new Integer(stringStringTuple2._1), new Integer(stringStringTuple2._2));
                            }
                        });
                    }
                })
                .reduceByKey(Integer::sum);

        countDStream.foreachRDD(v -> {
            v.foreach(record -> {
                String sql = String.format("UPDATE `post` SET likes = likes + %s WHERE id=%d", record._2, record._1);
                System.out.println(sql);
            });
            log.info("一批次数据流处理完： {}", v);
        });
        jsc.start();
    }

}
