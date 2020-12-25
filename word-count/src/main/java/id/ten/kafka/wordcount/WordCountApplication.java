package id.ten.kafka.wordcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.serializer.support.SerializationDelegate;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class WordCountApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(WordCountApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-application");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		KStreamBuilder builder = new KStreamBuilder();

		// 1. stream from kafka
		KStream<String, String> wordCountInput = builder.stream("word-count-input");

		KTable<String, Long> wordCounts =  wordCountInput.mapValues(textLine -> textLine.toLowerCase())
		.flatMapValues(lowercaseTextLine -> Arrays.asList(lowercaseTextLine.split(" ")))
		.selectKey((ignoredkey, word) -> word)
		.groupByKey()
		.count("Counts");

		// send to kafka stream
		wordCounts.to(Serdes.String(), Serdes.Long(), "word-count-output");

		KafkaStreams streams = new KafkaStreams(builder, config);
		streams.start();

		System.out.println(streams.toString());

		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
;	}
}
