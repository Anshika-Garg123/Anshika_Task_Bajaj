package Anshika_Task.Anshika_Task_Bajaj.service;


import Anshika_Task.Anshika_Task_Bajaj.model.GenerateWebhookResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

@Component
public class QualificationService implements CommandLineRunner {

    private final RestTemplate restTemplate;

    @Value("${bfhl.generateWebhook.url}")
    private String generateWebhookUrl;

    @Value("${bfhl.name}")
    private String name;

    @Value("${bfhl.regNo}")
    private String regNo;

    @Value("${bfhl.email}")
    private String email;

    @Value("${bfhl.submitWebhook.url}")
    private String fallbackSubmitUrl;

    public QualificationService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public void run(String... args) {
        try {
            System.out.println("\n=== Starting BFHL Task Execution ===");
            System.out.println("Using Registration Number: " + regNo);

            Map<String, String> requestBody = Map.of(
                    "name", name,
                    "regNo", regNo,
                    "email", email
            );

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);

            ResponseEntity<GenerateWebhookResponse> response =
                    restTemplate.postForEntity(generateWebhookUrl, request, GenerateWebhookResponse.class);

            if (!response.getStatusCode().is2xxSuccessful()) {
                System.err.println("generateWebhook API failed with status: " + response.getStatusCode());
                return;
            }

            GenerateWebhookResponse webhookResponse = response.getBody();

            if (webhookResponse == null) {
                System.err.println("generateWebhook API returned null response body!");
                return;
            }

            String webhookUrl = webhookResponse.getWebhook();
            String accessToken = webhookResponse.getAccessToken();

            System.out.println("Webhook URL received: " + webhookUrl);
            System.out.println("Access Token received: <hidden>");

            String finalSql = buildFinalSqlQuery();

            System.out.println("\nFinal SQL Query to be submitted:");
            System.out.println(finalSql);

            HttpHeaders authHeaders = new HttpHeaders();
            authHeaders.setContentType(MediaType.APPLICATION_JSON);
            authHeaders.set("Authorization", accessToken);

            Map<String, String> submitPayload = Map.of(
                    "finalQuery", finalSql
            );

            HttpEntity<Map<String, String>> submitRequest =
                    new HttpEntity<>(submitPayload, authHeaders);

            String targetWebhook = (webhookUrl == null || webhookUrl.isBlank())
                    ? fallbackSubmitUrl
                    : webhookUrl;

            ResponseEntity<String> submitResponse =
                    restTemplate.postForEntity(targetWebhook, submitRequest, String.class);

            System.out.println("\nSubmission Status: " + submitResponse.getStatusCode());
            System.out.println("Submission Response Body: " + submitResponse.getBody());

            System.out.println("\n=== BFHL Task Completed Successfully ===");

        } catch (Exception e) {
            System.err.println("\nERROR during task execution:");
            e.printStackTrace();
        }
    }

    // STEP 5 — Final SQL Query for Question 1
    private String buildFinalSqlQuery() {
        return """
                SELECT 
                    d.DEPARTMENT_NAME,
                    t.max_salary AS SALARY,
                    CONCAT(e.FIRST_NAME, ' ', e.LAST_NAME) AS EMPLOYEE_NAME,
                    TIMESTAMPDIFF(YEAR, e.DOB, CURDATE()) AS AGE
                FROM DEPARTMENT d
                JOIN (
                    SELECT 
                        e.DEPARTMENT,
                        p.EMP_ID,
                        SUM(p.AMOUNT) AS max_salary
                    FROM PAYMENTS p
                    JOIN EMPLOYEE e ON e.EMP_ID = p.EMP_ID
                    WHERE DAY(p.PAYMENT_TIME) != 1
                    GROUP BY e.DEPARTMENT, p.EMP_ID
                ) t ON t.DEPARTMENT = d.DEPARTMENT_ID
                JOIN EMPLOYEE e ON e.EMP_ID = t.EMP_ID
                WHERE t.max_salary IN (
                    SELECT 
                        MAX(sub.total_salary)
                    FROM (
                        SELECT 
                            e2.DEPARTMENT,
                            SUM(p2.AMOUNT) AS total_salary
                        FROM PAYMENTS p2
                        JOIN EMPLOYEE e2 ON e2.EMP_ID = p2.EMP_ID
                        WHERE DAY(p2.PAYMENT_TIME) != 1
                        GROUP BY e2.DEPARTMENT, p2.EMP_ID
                    ) sub
                    WHERE sub.DEPARTMENT = d.DEPARTMENT_ID
                );
                """;
    }
}
