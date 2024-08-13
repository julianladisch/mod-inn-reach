package org.folio.innreach.batch.contribution;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.folio.innreach.batch.contribution.listener.ContributionExceptionListener;
import org.folio.innreach.batch.contribution.service.ContributionJobRunner;
import org.folio.innreach.config.RetryConfig;
import org.folio.spring.config.properties.FolioEnvironment;
import org.folio.spring.tools.kafka.KafkaUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import org.folio.innreach.batch.KafkaItemReader;
import org.folio.innreach.config.props.ContributionJobProperties;
import org.folio.innreach.domain.dto.folio.inventorystorage.InstanceIterationEvent;

@Component
@RequiredArgsConstructor
@Log4j2
public class IterationEventReaderFactory {

  public static final String ITERATION_JOB_ID_HEADER = "iteration-job-id";

  public static final Consumer<ConsumerRecord<String, InstanceIterationEvent>> CONSUMER_REC_PROCESSOR =
    rec -> {
      var event = rec.value();
      var jobId = getJobId(rec);
      event.setInstanceId(UUID.fromString(rec.key()));
      event.setJobId(jobId);
    };

  private final KafkaProperties kafkaProperties;
  private final FolioEnvironment folioEnv;
  private final ContributionJobProperties jobProperties;
  private final ObjectMapper mapper;

  @Value(value = "${kafka.custom-offset}")
  private String DEFAULT_OFFSET;

  @Value(value = "${kafka.custom-concurrency}")
  private int KAFKA_EVENTS_CONCURRENCY;

  @Qualifier("instanceExceptionListener")
  private final ContributionExceptionListener contributionExceptionListener;

  private final RetryConfig retryConfig;

  public KafkaItemReader<String, InstanceIterationEvent> createReader(String tenantId) {
    Properties props = new Properties();
    props.putAll(kafkaProperties.buildConsumerProperties(null));
    props.put(GROUP_ID_CONFIG, jobProperties.getReaderGroupId());

    var topic = getTopicName(tenantId);

    var reader = new KafkaItemReader<>(props, topic, keyDeserializer(), valueDeserializer());
    reader.setPollTimeout(Duration.ofSeconds(jobProperties.getReaderPollTimeoutSec()));
    reader.setRecordProcessor(CONSUMER_REC_PROCESSOR);

    return reader;
  }

  private Deserializer<InstanceIterationEvent> valueDeserializer() {
    JsonDeserializer<InstanceIterationEvent> deserializer = new JsonDeserializer<>(InstanceIterationEvent.class, mapper);
    deserializer.setUseTypeHeaders(false);
    deserializer.addTrustedPackages("*");

    return deserializer;
  }

  private Deserializer<String> keyDeserializer() {
    return new StringDeserializer();
  }

  private static UUID getJobId(ConsumerRecord<String, InstanceIterationEvent> rec) {
    return UUID.fromString(new String(rec.headers().lastHeader(ITERATION_JOB_ID_HEADER).value()));
  }

  public InitialContributionJobConsumerContainer createInitialContributionConsumerContainer(String tenantId, ContributionJobRunner contributionJobRunner) {
    log.info("Default offset: {}", DEFAULT_OFFSET);
    var consumerProperties = kafkaProperties.buildConsumerProperties(null);
    consumerProperties.put(GROUP_ID_CONFIG, jobProperties.getReaderGroupId());
    consumerProperties.put(AUTO_OFFSET_RESET_CONFIG, DEFAULT_OFFSET);

    var topic = getTopicName(tenantId);
    return new InitialContributionJobConsumerContainer(consumerProperties,topic,keyDeserializer(),valueDeserializer(),
      retryConfig.getInterval(), retryConfig.getMaxAttempts(), KAFKA_EVENTS_CONCURRENCY,contributionExceptionListener,contributionJobRunner);
  }

  public String getTopicName(String tenantId) {
    return KafkaUtils.getTenantTopicName(jobProperties.getReaderTopic(), folioEnv.getEnvironment(), tenantId);
  }

}
