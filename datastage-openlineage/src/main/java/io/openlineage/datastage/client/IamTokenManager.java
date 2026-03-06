package io.openlineage.datastage.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

import java.time.Instant;
import java.util.Map;

public class IamTokenManager {

    private static final Logger log = LoggerFactory.getLogger(IamTokenManager.class);
    private static final String IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token";
    private static final long REFRESH_MARGIN_SECONDS = 300; // 5 minutes before expiry

    private final String apiKey;
    private final RestClient restClient;

    private String cachedToken;
    private Instant tokenExpiry = Instant.EPOCH;

    public IamTokenManager(String apiKey, RestClient.Builder restClientBuilder) {
        this.apiKey = apiKey;
        this.restClient = restClientBuilder.build();
    }

    public synchronized String getToken() {
        if (cachedToken == null || Instant.now().isAfter(tokenExpiry.minusSeconds(REFRESH_MARGIN_SECONDS))) {
            refresh();
        }
        return cachedToken;
    }

    private void refresh() {
        log.debug("Refreshing IAM bearer token");
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> response = restClient.post()
                    .uri(IAM_TOKEN_URL)
                    .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                    .body("grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=" + apiKey)
                    .retrieve()
                    .body(Map.class);

            if (response != null) {
                this.cachedToken = (String) response.get("access_token");
                int expiresIn = (Integer) response.get("expires_in");
                this.tokenExpiry = Instant.now().plusSeconds(expiresIn);
                log.debug("IAM token refreshed, expires in {} seconds", expiresIn);
            }
        } catch (Exception e) {
            log.error("Failed to refresh IAM token", e);
            throw new RuntimeException("IAM token refresh failed", e);
        }
    }
}
