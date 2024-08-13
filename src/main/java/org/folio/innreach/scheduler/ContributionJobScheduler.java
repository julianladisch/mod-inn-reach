package org.folio.innreach.scheduler;

import com.google.common.cache.Cache;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.innreach.batch.contribution.service.InitialContributionEventProcessor;
import org.folio.innreach.batch.contribution.service.OngoingContributionEventProcessor;
import org.folio.innreach.domain.entity.TenantInfo;
import org.folio.innreach.domain.service.ContributionService;
import org.folio.innreach.domain.service.impl.TenantScopedExecutionService;
import org.folio.innreach.repository.JobExecutionStatusRepository;
import org.folio.innreach.repository.OngoingContributionStatusRepository;
import org.folio.innreach.repository.TenantInfoRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;


@Service
@RequiredArgsConstructor
@Log4j2
public class ContributionJobScheduler {
  private final TenantScopedExecutionService tenantScopedExecutionService;
  private final JobExecutionStatusRepository jobExecutionStatusRepository;
  private final InitialContributionEventProcessor eventProcessor;
  private final TenantInfoRepository tenantRepository;
  private final ContributionService contributionService;
  private final OngoingContributionStatusRepository ongoingContributionStatusRepository;
  private final OngoingContributionEventProcessor ongoingContributionEventProcessor;
  @Value(value = "${contribution.fetch-limit}")
  private int recordLimit;
  @Value(value = "${contribution.item-pause}")
  private double itemPause;
  private final Cache<String, List<String>> tenantDetailsCache;

  @PostConstruct
  public void postConstruct() {
    try {
      this.loadTenants()
        .forEach(tenantId ->
          tenantScopedExecutionService.runTenantScoped(tenantId, () -> {
            jobExecutionStatusRepository.updateInProgressRecordsToReady();
            ongoingContributionStatusRepository.updateInProgressToReady();
          }));
      log.info("postConstruct:: Status of the records updated to Ready successfully");
    } catch (Exception ex) {
      log.warn("postConstruct:: Error while updating the record status from In progress to ready {}", ex.getMessage());
    }
  }

  @Scheduled(fixedDelayString = "${contribution.scheduler.fixed-delay}",
    initialDelayString = "${contribution.scheduler.initial-delay}")
  public void processInitialContributionEvents() {
    List<String> tenants = loadTenants();
    log.info("processInitialContributionEvents :: tenantsList {}", tenants);
    tenants.forEach(tenant ->
      tenantScopedExecutionService.runTenantScoped(tenant,
        () -> {
          try {
            long inProgressCount = jobExecutionStatusRepository.getInProgressRecordsCount();
            if(recordLimit > inProgressCount) {
              log.info("processInitialContributionEvents:: Fetching new set of records");
              jobExecutionStatusRepository.updateAndFetchJobExecutionRecordsByStatus(recordLimit, itemPause)
                .forEach(eventProcessor::processInitialContributionEvents);
            } else {
              log.info("processInitialContributionEvents:: unable to fetch new records, " +
                "as inProgress count {} is greater than fetchLimit {}", inProgressCount, recordLimit);
            }
            contributionService.updateStatisticsAndContributionStatus();
          } catch (Exception ex) {
            log.warn("Exception caught while processing Initial contribution for tenant {} {} ", tenant, ex.getMessage());
          }
        }
      ));
  }

  @Scheduled(fixedDelayString = "${contribution.scheduler.fixed-delay}",
    initialDelayString = "${contribution.scheduler.initial-delay}")
  public void processOngoingContributionEvents() {
    List<String> tenants = loadTenants();
    log.info("processOngoingContributionEvents :: tenantsList {}", tenants);
    tenants.forEach(tenant ->
      tenantScopedExecutionService.runTenantScoped(tenant,
        () -> {
          try {
            long inProgressCount = ongoingContributionStatusRepository.getInProgressRecordsCount();
            if(recordLimit > inProgressCount) {
              log.info("processOngoingContributionEvents:: Fetching new set of records");
              ongoingContributionStatusRepository.updateAndFetchOngoingContributionRecordsByStatus(recordLimit)
                .forEach(ongoingContributionEventProcessor::processOngoingContribution);
            } else {
              log.info("processOngoingContributionEvents:: unable to fetch new records, " +
                "as inProgress count {} is greater than fetchLimit {}", inProgressCount, recordLimit);
            }
          } catch (Exception ex) {
            log.warn("processOngoingContributionEvents:: Exception caught while processing ongoing contribution for tenant {} {} ", tenant, ex.getMessage());
          }
        }
      ));
  }

  private List<String> loadTenants() {
    String tenantCacheKey = "tenantList";
    var tenantList = tenantDetailsCache.getIfPresent(tenantCacheKey);
    if (tenantList == null) {
      log.info("Tenant details Refreshed");
      tenantList = tenantRepository.findAll().
        stream().map(TenantInfo::getTenantId).
        distinct().toList();
      tenantDetailsCache.put(tenantCacheKey, tenantList);
    }
    return tenantList;
  }
}
