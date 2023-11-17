package org.folio.innreach.repository;

import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.folio.innreach.client.UsersClient;
import org.folio.innreach.domain.service.UserService;
import org.folio.innreach.domain.service.impl.UserServiceImpl;
import org.folio.spring.FolioExecutionContext;

@DataJpaTest
@Testcontainers
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@DirtiesContext
@ActiveProfiles("test")
abstract class BaseRepositoryTest {

  @Container
  static final PostgreSQLContainer postgresqlContainer = new PostgreSQLContainer(System.getenv().getOrDefault("TESTCONTAINERS_POSTGRES_IMAGE", "postgres:11-alpine"))
      .withDatabaseName("db")
      .withUsername("postgres")
      .withPassword("postgres");

  @DynamicPropertySource
  static void setDatasourceProperties(DynamicPropertyRegistry propertyRegistry) {
    propertyRegistry.add("spring.datasource.url", postgresqlContainer::getJdbcUrl);
    propertyRegistry.add("spring.datasource.password", postgresqlContainer::getPassword);
    propertyRegistry.add("spring.datasource.username", postgresqlContainer::getUsername);
  }

  @MockBean
  private FolioExecutionContext folioExecutionContext;
  @MockBean
  private UsersClient usersClient;

  @TestConfiguration
  static class BaseRepositoryConfiguration {

    @Bean
    public UserService userService(UsersClient userClient) {
      return new UserServiceImpl(userClient);
    }

  }

}
