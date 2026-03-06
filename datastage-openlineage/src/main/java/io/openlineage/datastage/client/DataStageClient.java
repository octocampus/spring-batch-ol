package io.openlineage.datastage.client;

import io.openlineage.datastage.client.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.client.RestClient;

public class DataStageClient {

    private static final Logger log = LoggerFactory.getLogger(DataStageClient.class);

    private final RestClient restClient;
    private final IamTokenManager tokenManager;
    private final String projectId;

    public DataStageClient(String serviceUrl, String projectId, IamTokenManager tokenManager,
                           RestClient.Builder restClientBuilder) {
        this.restClient = restClientBuilder.baseUrl(serviceUrl).build();
        this.tokenManager = tokenManager;
        this.projectId = projectId;
    }

    public JobListResponse listJobs() {
        log.debug("Listing DataStage jobs for project {}", projectId);
        return restClient.get()
                .uri("/v2/jobs?project_id={pid}", projectId)
                .header("Authorization", "Bearer " + tokenManager.getToken())
                .retrieve()
                .body(JobListResponse.class);
    }

    public JobRunListResponse listRuns(String jobId) {
        log.debug("Listing runs for job {}", jobId);
        return restClient.get()
                .uri("/v2/jobs/{jobId}/runs?project_id={pid}", jobId, projectId)
                .header("Authorization", "Bearer " + tokenManager.getToken())
                .retrieve()
                .body(JobRunListResponse.class);
    }

    public JobRunDetail getRunDetail(String jobId, String runId) {
        log.debug("Getting run detail for job {} run {}", jobId, runId);
        return restClient.get()
                .uri("/v2/jobs/{jobId}/runs/{runId}?project_id={pid}", jobId, runId, projectId)
                .header("Authorization", "Bearer " + tokenManager.getToken())
                .retrieve()
                .body(JobRunDetail.class);
    }

    public FlowDefinition getFlowDefinition(String flowId) {
        log.debug("Getting flow definition {}", flowId);
        return restClient.get()
                .uri("/v3/data_intg/flows/{flowId}?project_id={pid}", flowId, projectId)
                .header("Authorization", "Bearer " + tokenManager.getToken())
                .retrieve()
                .body(FlowDefinition.class);
    }

    public ConnectionDetail getConnection(String connectionId) {
        log.debug("Getting connection {}", connectionId);
        return restClient.get()
                .uri("/v2/connections/{connId}?project_id={pid}", connectionId, projectId)
                .header("Authorization", "Bearer " + tokenManager.getToken())
                .retrieve()
                .body(ConnectionDetail.class);
    }
}
