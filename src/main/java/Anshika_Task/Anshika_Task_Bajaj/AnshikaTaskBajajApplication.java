package Anshika_Task.Anshika_Task_Bajaj;

import Anshika_Task.Anshika_Task_Bajaj.service.QualificationService;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import org.springframework.boot.WebApplicationType;
import java.util.Map;
import java.util.logging.Logger;

@SpringBootApplication
public class AnshikaTaskBajajApplication {

    private static final Logger logger = Logger.getLogger(AnshikaTaskBajajApplication.class.getName());

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(AnshikaTaskBajajApplication.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        application.run(args);
    }

    @Bean
    public CommandLineRunner runTask(RestTemplate restTemplate) {
        return args -> {
            logger.info("Starting Task: Generating Webhook and Submitting SQL Query...");

            String webhookUrl = "https://bfhldevapigw.healthrx.co.in/hiring/generateWebhook/JAVA";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, String> requestBody = Map.of(
                    "name", "Anshika",
                    "regNo", "22BCE10911",
                    "email", "anshikagarg2105@gmail.com"
            );
            HttpEntity<Map<String, String>> request = new HttpEntity<>(requestBody, headers);

            ResponseEntity<Map> response;
            try {
                response = restTemplate.exchange(
                        webhookUrl,
                        HttpMethod.POST,
                        request,
                        Map.class
                );
            } catch (Exception e) {
                logger.severe("Error generating webhook: " + e.getMessage());
                return;
            }

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Map<String, String> responseBody = response.getBody();
                String submissionUrl = responseBody.get("webhookURL");
                String accessToken = responseBody.get("accessToken");

                if (submissionUrl != null && accessToken != null) {
                    logger.info("Webhook generated successfully. AccessToken received.");

                    String finalSqlQuery = getQuestion1SqlQuery();
                    submitSolution(restTemplate, accessToken, finalSqlQuery);
                } else {
                    logger.severe("Webhook response missing 'webhookURL' or 'accessToken'.");
                }
            } else {
                logger.severe("Failed to generate webhook. Status code: " + response.getStatusCode());
            }

            logger.info("Task flow completed. Application finished.");
        };
    }


    private String getQuestion1SqlQuery() {
        return """
            WITH FilteredPayments AS (
                -- Filter out payments made on the 1st day of any month
                SELECT
                    EMP_ID,
                    AMOUNT AS SALARY,
                    ROW_NUMBER() OVER (PARTITION BY EMP_ID ORDER BY AMOUNT DESC, PAYMENT_TIME DESC) as rn
                FROM PAYMENTS
                -- This condition filters out the 1st day of the month
                WHERE CAST(PAYMENT_TIME AS DATE) != DATE_TRUNC('month', CAST(PAYMENT_TIME AS DATE))
            ),
            EmployeeSalaryRank AS (
                -- Find the highest salary among the filtered payments for each employee
                SELECT
                    e."EMP ID" as EMP_ID,
                    e."DEPARTMENT" as DEPT_ID,
                    fp.SALARY
                FROM EMPLOYEE e
                JOIN FilteredPayments fp ON e."EMP ID" = fp.EMP_ID
                WHERE fp.rn = 1
            ),
            RankedDepartmentSalaries AS (
                -- Rank the highest salaries per employee within their department
                SELECT
                    esr.DEPT_ID,
                    esr.SALARY,
                    e."FIRST NAM E",
                    e."LAST NAM E",
                    e.DOB,
                    RANK() OVER (PARTITION BY esr.DEPT_ID ORDER BY esr.SALARY DESC) as DeptRank
                FROM EmployeeSalaryRank esr
                JOIN EMPLOYEE e ON esr.EMP_ID = e.EMP_ID
            )
            -- Final selection: Get only the highest salaried employee (Rank = 1) per department
            SELECT
                d."DEPARTMENT NAME" AS DEPARTMENT_NAME,
                rds.SALARY,
                rds."FIRST NAM E" || ' ' || rds."LAST NAM E" AS EMPLOYEE_NAME, -- Combine name
                CAST(strftime('%Y.%m%d', 'now') - strftime('%Y.%m%d', rds.DOB) AS INT) AS AGE -- Calculate AGE
            FROM RankedDepartmentSalaries rds
            JOIN DEPARTMENT d ON rds.DEPT_ID = d."DEPARTMENT ID"
            WHERE rds.DeptRank = 1
            ORDER BY d."DEPARTMENT NAME";
            """;
    }


    private void submitSolution(RestTemplate restTemplate, String accessToken, String finalQuery) {
        String finalSubmissionUrl = "https://bfhldevapigw.healthrx.co.in/hiring/testWebhook/JAVA";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(accessToken);

        Map<String, String> submissionBody = Map.of(
                "finalQuery", finalQuery
        );
        HttpEntity<Map<String, String>> request = new HttpEntity<>(submissionBody, headers);

        try {
            ResponseEntity<String> submissionResponse = restTemplate.exchange(
                    finalSubmissionUrl,
                    HttpMethod.POST,
                    request,
                    String.class
            );
            logger.info("Submission Response Status: " + submissionResponse.getStatusCode());
            logger.info("Submission Response Body: " + submissionResponse.getBody());
        } catch (Exception e) {
            logger.severe("Error submitting solution: " + e.getMessage());
        }
    }
}