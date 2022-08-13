package com.icelancer.batch

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.batch.test.JobLauncherTestUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@SpringBootTest(classes = [FirstBatchTest::class, FirstBatchTest.BatchConfig::class])
class FirstBatchTest(
    @Autowired
    private val jobLauncherTestUtils: JobLauncherTestUtils
) {

    @Test
    fun test() {
        val result = jobLauncherTestUtils.launchJob()
        Assertions.assertEquals(result.status, BatchStatus.COMPLETED)
    }

    @EnableBatchProcessing
    @Configuration
    class BatchConfig(
        @Autowired
        val jobBuilderFactory: JobBuilderFactory,

        @Autowired
        val stepBuilderFactory: StepBuilderFactory
    ) {
        @Bean
        fun firstJob(firstStep: Step): Job {
            return jobBuilderFactory.get("firstBatchJob")
                .start(firstStep)
                .build()
        }

        @Bean
        fun firstStep(): Step {
            var count = 0
            return stepBuilderFactory.get("firstStep")
                .tasklet(
                    Tasklet { contribution, chunkContext ->
                        println("first Batch: ${count++}")
                        if (count < 5) {
                            RepeatStatus.CONTINUABLE
                        } else {
                            RepeatStatus.FINISHED
                        }
                    }
                )
                .build()
        }

        @Bean
        fun jobLauncherTestUtils(): JobLauncherTestUtils {
            return JobLauncherTestUtils()
        }
    }
}
