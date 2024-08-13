package org.folio.innreach.batch.contribution;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.folio.innreach.batch.contribution.service.ContributionJobRunner;
import org.folio.innreach.config.RetryConfig;
import org.folio.spring.config.properties.FolioEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

import org.folio.innreach.config.props.ContributionJobProperties;

@ExtendWith(MockitoExtension.class)
class IterationEventReaderFactoryTest {

  @Mock
  private KafkaProperties kafkaProperties;
  @Mock
  private FolioEnvironment folioEnv;
  @Mock
  private ContributionJobProperties jobProperties;
  @Mock
  private ObjectMapper mapper;
  @Mock
  ContributionJobRunner contributionJobRunner;
  @Mock
  RetryConfig retryConfig;

  @InjectMocks
  private IterationEventReaderFactory factory;

  @Test
  void createReader() {
    when(jobProperties.getReaderGroupId()).thenReturn("topic");

    var reader = factory.createReader("test");

    assertNotNull(reader);

    verify(kafkaProperties).buildConsumerProperties(null);
    verify(folioEnv).getEnvironment();
    verify(jobProperties).getReaderTopic();
    verify(jobProperties).getReaderPollTimeoutSec();
  }

  @Test
  void createInitialContributionConsumerContainer() {
    var consumerContainer = factory.createInitialContributionConsumerContainer("test",contributionJobRunner);
    assertNotNull(consumerContainer);

    verify(kafkaProperties).buildConsumerProperties(null);
    verify(folioEnv).getEnvironment();
    verify(jobProperties).getReaderTopic();
  }

}
