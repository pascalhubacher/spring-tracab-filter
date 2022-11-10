package com.mas2022datascience.springtracabfilter.processor;

import com.mas2022datascience.avro.v1.Frame;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
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

  @Value(value = "${filter.inPhase}")
  private Boolean inPhase;

  @Value(value = "${filter.isBallInPlay}")
  private String isBallInPlay;


  @Bean
  public KStream<String, Frame> kStream(StreamsBuilder kStreamBuilder) {

    final Serde<Frame> frameSerde = new SpecificAvroSerde<>();

    // the builder is used to construct the topology
    KStream<String, Frame> stream = kStreamBuilder.stream(topicIn);

    stream
        .filter((matchId, actualFrame) -> checkInPhases(actualFrame) || !inPhase)
        .filter((matchId, actualFrame) -> checkIsBallInPlay(actualFrame, isBallInPlay))
        .to(topicOut);

    return stream;

  }

  /**
   * Convertes the utc string of type "yyyy-MM-dd'T'HH:mm:ss.SSS" to epoc time in milliseconds.
   * @param utcString of type String of format 'yyyy-MM-dd'T'HH:mm:ss.SSS'
   * @return epoc time in milliseconds
   */
  private static long utcString2epocMs(String utcString) {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC);

    return Instant.from(fmt.parse(utcString)).toEpochMilli();
  }

  /**
   * checks if the utcString timestamp of the actualFrame is within the phases.
   * If yes then it returns true. Otherwise, it returns false.
   * @param actualFrame of type Frame
   * @return of type boolean
   */
  private boolean checkInPhases(Frame actualFrame) {
    long firstHalfStart = utcString2epocMs(actualFrame.getPhases().get(0).getStart());
    long firstHalfEnd = utcString2epocMs(actualFrame.getPhases().get(0).getEnd());
    long secondHalfStart = utcString2epocMs(actualFrame.getPhases().get(1).getStart());
    long secondHalfEnd = utcString2epocMs(actualFrame.getPhases().get(1).getEnd());

    long actualTimestamp = utcString2epocMs(actualFrame.getUtc());

    if (actualTimestamp > firstHalfStart && actualTimestamp < firstHalfEnd) {
      return true;
    }
    return actualTimestamp > secondHalfStart && actualTimestamp < secondHalfEnd;
  }

  /**
   * checks if the isBallInPlay of the actualFrame is set to 0 or 1.
   * If filter set to all or anything else then it returns always true
   * @param actualFrame of type frame
   * @param isBallInPlay is of type string
   * @return of type boolean
   */
  private boolean checkIsBallInPlay(Frame actualFrame, String isBallInPlay) {
    switch (isBallInPlay) {
      case "0":
      case "1":
        if (!isBallInPlay.equals(String.valueOf(actualFrame.getIsBallInPlay()))) {
          return false;
        }
      default:
        return true;
    }
  }

}


