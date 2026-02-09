package io.openlineage.batch.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

public class StatsTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(StatsTasklet.class);

    private final JdbcTemplate jdbcTemplate;

    public StatsTasklet(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        log.info("--- People Analytics Summary ---");
        jdbcTemplate.query(
                "SELECT category, COUNT(*) AS cnt, AVG(age) AS avg_age FROM enriched_people GROUP BY category",
                (rs, rowNum) -> {
                    String category = rs.getString("category");
                    long count = rs.getLong("cnt");
                    double avgAge = rs.getDouble("avg_age");
                    log.info("  Category: {} | Count: {} | Avg Age: {}", category, count, String.format("%.1f", avgAge));
                    return null;
                });
        log.info("--------------------------------");
        return RepeatStatus.FINISHED;
    }
}
