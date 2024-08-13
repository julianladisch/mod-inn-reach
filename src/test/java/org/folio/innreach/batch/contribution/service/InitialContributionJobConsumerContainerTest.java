package org.folio.innreach.batch.contribution.service;

import feign.FeignException;
import org.folio.innreach.external.exception.ServiceSuspendedException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.Mock;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.folio.innreach.batch.contribution.ContributionJobContext;
import org.folio.innreach.batch.contribution.InitialContributionJobConsumerContainer;
import org.folio.innreach.batch.contribution.listener.ContributionExceptionListener;
import org.folio.innreach.config.RetryConfig;
import org.folio.innreach.config.props.ContributionJobProperties;
import org.folio.innreach.domain.dto.folio.inventorystorage.InstanceIterationEvent;
import org.folio.innreach.domain.listener.base.BaseKafkaApiTest;
import org.folio.innreach.domain.service.impl.ContributionServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.messaging.support.MessageBuilder;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.awaitility.Awaitility.await;
import static org.folio.innreach.batch.contribution.ContributionJobContextManager.beginContributionJobContext;
import static org.folio.innreach.batch.contribution.IterationEventReaderFactory.ITERATION_JOB_ID_HEADER;
import static org.folio.innreach.fixture.ContributionFixture.createContributionJobContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;

class InitialContributionJobConsumerContainerTest extends BaseKafkaApiTest{

  public static final String TOPIC = "folio.contrib.tester.innreach";
  public static final String TEST_TENANT = "testTenant";

  private static final ContributionJobContext JOB_CONTEXT = createContributionJobContext();

  private final Long maxInterval = 2000L;

  private final Long maxAttempt = 2L;
  @Autowired
  private KafkaProperties kafkaProperties;

  @Autowired
  private ObjectMapper mapper;

  @Autowired
  private ContributionJobProperties jobProperties;

  @Autowired
  private RetryConfig retryConfig;

  @Mock
  ContributionJobRunner contributionJobRunner;

  @Autowired
  ContributionServiceImpl contributionService;

  @Mock
  ContributionExceptionListener contributionExceptionListener;

  @BeforeEach
  void clearMap() {
    InitialContributionJobConsumerContainer.consumersMap.clear();
    beginContributionJobContext(JOB_CONTEXT);
  }

  @Test
  void testStartAndStopConsumerIfServiceException() throws InterruptedException {
    var topicName = getTopicName();
    var context = prepareContext();
    var initialContributionJobConsumerContainer = prepareContributionJobConsumerContainer(topicName);
    InitialContributionMessageListener initialContributionMessageListener = prepareInitialContributionMessageListener(context);

    this.produceEvent(topicName);

    doNothing().when(contributionExceptionListener).logWriteError(any(),any());
    doNothing().when(contributionJobRunner).stopContribution(any());
    doNothing().when(contributionJobRunner).cancelContributionIfRetryExhausted(any());

    doThrow(ServiceSuspendedException.class).when(contributionJobRunner)
      .runInitialContribution(any(), any());

    initialContributionJobConsumerContainer.tryStartOrCreateConsumer(initialContributionMessageListener);

    await().atMost(Duration.ofSeconds(10L)).until(()->!InitialContributionJobConsumerContainer.consumersMap.get(topicName).isRunning());
  }

  @Test
  void testStartAndStopConsumerIfFeignException() throws InterruptedException {
    var topicName = getTopicName();
    var context = prepareContext();
    var initialContributionJobConsumerContainer = prepareContributionJobConsumerContainer(topicName);
    InitialContributionMessageListener initialContributionMessageListener = prepareInitialContributionMessageListener(context);

    this.produceEvent(topicName);

    doNothing().when(contributionExceptionListener).logWriteError(any(),any());
    doNothing().when(contributionJobRunner).stopContribution(any());
    doNothing().when(contributionJobRunner).cancelContributionIfRetryExhausted(any());

    doThrow(FeignException.class).when(contributionJobRunner)
      .runInitialContribution(any(), any());

    initialContributionJobConsumerContainer.tryStartOrCreateConsumer(initialContributionMessageListener);

    await().atMost(Duration.ofSeconds(60L)).until(()->!InitialContributionJobConsumerContainer.consumersMap.get(topicName).isRunning());
  }

  @Test
  void testStartOrCreateConsumer() throws InterruptedException {
    var topicName = getTopicName();
    var context = prepareContext();
    var initialContributionJobConsumerContainer = prepareContributionJobConsumerContainer(topicName);
    InitialContributionMessageListener initialContributionMessageListener = prepareInitialContributionMessageListener(context);

    this.produceEvent(topicName);

    initialContributionJobConsumerContainer.tryStartOrCreateConsumer(initialContributionMessageListener);

    doNothing().when(contributionJobRunner).runInitialContribution(any(), any());

    Assertions.assertNotNull(InitialContributionJobConsumerContainer.consumersMap.get(topicName));

    Assertions.assertEquals(1,InitialContributionJobConsumerContainer.consumersMap.size());
  }

  @Test
  void stopConsumer() {
    var topicName = getTopicName();
    var context = prepareContext();

    doNothing().when(contributionJobRunner).stopContribution(any());
    doNothing().when(contributionJobRunner).cancelContributionIfRetryExhausted(any());

    var initialContributionJobConsumerContainer = prepareContributionJobConsumerContainer(topicName);
    InitialContributionMessageListener initialContributionMessageListener = prepareInitialContributionMessageListener(context);

    initialContributionJobConsumerContainer.tryStartOrCreateConsumer(initialContributionMessageListener);

    InitialContributionJobConsumerContainer.stopConsumer(topicName);
    Assertions.assertNotNull(InitialContributionJobConsumerContainer.consumersMap.get(topicName));
  }

  @NotNull
  private InitialContributionMessageListener prepareInitialContributionMessageListener(ContributionJobContext context) {
    var contributionProcessor = new ContributionProcessor(contributionJobRunner);
    return new InitialContributionMessageListener(contributionProcessor);
  }

  @Test
  void testContainerIfRunning() {
    var topicName = getTopicName();
    var context = prepareContext();

    var consumerProperties = kafkaProperties.buildConsumerProperties(null);

    ConsumerFactory<String, InstanceIterationEvent> factory = new DefaultKafkaConsumerFactory<>(consumerProperties,keyDeserializer(),valueDeserializer());

    ContainerProperties containerProps = new ContainerProperties(topicName);

    containerProps.setPollTimeout(100);

    containerProps.setAckMode(ContainerProperties.AckMode.RECORD);

    var initialContributionJobConsumerContainer = prepareContributionJobConsumerContainer(topicName);
    var contributionProcessor = new ContributionProcessor(contributionJobRunner);

    var container = new ConcurrentMessageListenerContainer<>(factory,containerProps);

    InitialContributionMessageListener initialContributionMessageListener =
      new InitialContributionMessageListener(contributionProcessor);

    container.setupMessageListener(initialContributionMessageListener);

    InitialContributionJobConsumerContainer.consumersMap.put(topicName, container);

    initialContributionJobConsumerContainer.tryStartOrCreateConsumer(initialContributionMessageListener);
    doNothing().when(contributionJobRunner).runInitialContribution(any(), any());
    Assertions.assertNotNull(InitialContributionJobConsumerContainer.consumersMap.get(topicName));
  }

  private ContributionJobContext prepareContext() {
    return ContributionJobContext.builder()
      .contributionId(UUID.randomUUID())
      .iterationJobId(UUID.randomUUID())
      .centralServerId(UUID.randomUUID())
      .tenantId(TEST_TENANT)
      .build();
  }

  private void produceEvent(String tempTopic) {

    var payload = MessageBuilder
      .withPayload(createInstanceIterationEvent())
      .setHeader(ITERATION_JOB_ID_HEADER, UUID.randomUUID().toString()).setHeader(KafkaHeaders.TOPIC, tempTopic)
      .setHeader(KafkaHeaders.KEY, UUID.randomUUID().toString())
      .build();

    new KafkaTemplate<String, InstanceIterationEvent>(producerFactory()).send(payload);
  }

  public InitialContributionJobConsumerContainer prepareContributionJobConsumerContainer(String tempTopic) {
    var consumerProperties = kafkaProperties.buildConsumerProperties(null);
    consumerProperties.put(GROUP_ID_CONFIG, jobProperties.getReaderGroupId());

    return new InitialContributionJobConsumerContainer(consumerProperties, tempTopic, keyDeserializer(), valueDeserializer(), maxInterval, maxAttempt,
      2, contributionExceptionListener, contributionJobRunner);
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

  public InstanceIterationEvent createInstanceIterationEvent() {
    return InstanceIterationEvent.of(UUID.randomUUID(),"UPDATE","diku",UUID.randomUUID());
  }

  public <V> ProducerFactory<String, V> producerFactory() {
    Map<String, Object> props = new HashMap<>(kafkaProperties.buildProducerProperties(null));
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    return new DefaultKafkaProducerFactory<>(props);
  }

  public String getTopicName() {
    return TOPIC + UUID.randomUUID();
  }

}
