package example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ReadFromKafka {

    public static String TXN_ID = "TXN_ID";
    public static String TIMESTAMP = "TIME_STAMP";
    public static String CARD_TYPE = "CARD_TYPE";
    public static String CARD_STATUS = "CARD_STATUS";
    public static String TXN_AMT = "TXN_AMT";
    public static String CARD_NUMBER = "CARD_NUMBER";

    //// VARIABLES
    public static String KAFKA_CONSUMER_TOPIC = "input";
    public static String KAFKA_PRODUCER_TOPIC = "credit_card_95p";
    //// TEST IN CLUSTER
    public static String BOOTSTRAP_SERVER = "172.30.74.84:9092,172.30.74.85:9092,172.30.74.86:9092";
//    public static String BOOTSTRAP_SERVER = "poc01.kbtg:9092,poc02.kbtg:9092,poc03.kbtg:9092";
    //// TEST IN MY LOCAL
//    public static String BOOTSTRAP_SERVER = "localhost:9092";

    public static Logger LOG = LoggerFactory.getLogger(ReadFromKafka.class);

    public static void main(String[] args) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVER);
//        properties.setProperty("group.id", "flink-gid");

        //// READ FROM EARLIEST HERE
//        properties.setProperty("auto.offset.reset", "earliest");

        //// END VARIABLES
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        ////////////////////////////////////////////////////////////////
        //// RECEIVE JSON
        FlinkKafkaConsumer<ObjectNode> JsonSource = new FlinkKafkaConsumer(KAFKA_CONSUMER_TOPIC, new JSONKeyValueDeserializationSchema(false), properties);
        DataStream<Tuple7<Integer, Long, String, String, Double, String, Integer>> messageStream = env.addSource(JsonSource).flatMap(new FlatMapFunction<ObjectNode, Tuple7<Integer, Long, String, String, Double, String, Integer>>() {
            @Override
            public void flatMap(ObjectNode s, Collector<Tuple7<Integer, Long, String, String, Double, String, Integer>> collector) throws Exception {
                collector.collect(new Tuple7<Integer, Long, String, String, Double, String, Integer>(
                        s.get("value").get(TXN_ID).asInt(),
                        s.get("value").get(TIMESTAMP).asLong(),
                        s.get("value").get(CARD_TYPE).asText(),
                        s.get("value").get(CARD_STATUS).asText(),
                        s.get("value").get(TXN_AMT).asDouble(),
                        s.get("value").get(CARD_NUMBER).asText(),
                        0
                ));
            }
        });

        //// PRODUCT KAFKA
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer(KAFKA_PRODUCER_TOPIC, new ProducerStringSerializationSchema(KAFKA_PRODUCER_TOPIC), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        DataStream<Tuple6<Integer, Long, String, String, Double, String>> accessCounts = messageStream
                .keyBy(6).process(new CountWithTimeoutFunction());

        DataStreamSink<String> sendingToKafka = accessCounts.process(new ProcessFunction<Tuple6<Integer, Long, String, String, Double, String>, String>() {
            @Override
            public void processElement(Tuple6<Integer, Long, String, String, Double, String> stringLongLongTuple3, Context context, Collector<String> collector) throws Exception {
                collector.collect(
                        "{"
                        +",\"TXN_ID\":" + stringLongLongTuple3.f0
                        +",\"TIME_STAMP\":" + stringLongLongTuple3.f1
                        +",\"CARD_TYPE\":\"" + stringLongLongTuple3.f2 + "\""
                        +",\"CARD_STATUS\":\"" + stringLongLongTuple3.f3 + "\""
                        +",\"TXN_AMT\":" + stringLongLongTuple3.f4
                        +",\"CARD_NUMBER\":\"" + stringLongLongTuple3.f5 + "\""
                        +"}");
            }
        }).addSink(myProducer);

        env.execute("Flink Four Three");
    }
}
