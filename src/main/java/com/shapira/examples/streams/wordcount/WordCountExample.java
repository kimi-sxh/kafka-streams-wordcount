package com.shapira.examples.streams.wordcount;


import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCountExample {

    public static void main(String[] args){

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.153.125:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"kafka\" password=\"suxh19901212\";");

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // work-around for an issue around timing of creating internal topics
        // Fixed in Kafka 0.10.2.0
        // don't use in large production apps - this increases network load
        // props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("wordcount-input");
        final Pattern pattern = Pattern.compile("\\W+");
        KStream wordCounts = source.flatMapValues(value-> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object, Object>(value, value))
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count(Named.as("CountStore")).mapValues(value->Long.toString(value)).toStream();
        wordCounts.to("wordcount-output");


        // inspect what kinds of topology is created
        Topology topology = builder.build();
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);

        // This is for reset to work. Don't use in production - it causes the app to re-load the state from Kafka on every start
//        streams.cleanUp();

        try {
            streams.start();
            Thread.sleep(600000L);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
//        Thread.sleep(600000L);
//
//        streams.close();

    }
}
