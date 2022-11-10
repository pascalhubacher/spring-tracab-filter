package com.mas2022datascience.springtracabfilter.processor;

import com.mas2022datascience.avro.v1.Frame;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {
  @Value(value = "${topic.tracab-01.name}")
  private String topicIn;

  @Value(value = "${topic.tracab-02.name}")
  private String topicOut;

  @Bean
  public KStream<String, Frame> kStream(StreamsBuilder kStreamBuilder) {

    final Serde<Frame> frameSerde = new SpecificAvroSerde<>();

    // the builder is used to construct the topology
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, Frame> stream = builder.stream(topicIn,
        Consumed.with(Serdes.String(), frameSerde));

    stream
        .mapValues(valueFrame -> valueFrame)
        .to(topicOut);

    return stream;

  }
}


