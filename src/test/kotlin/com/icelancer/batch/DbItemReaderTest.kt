package com.icelancer.batch

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder
import org.springframework.batch.item.support.PassThroughItemProcessor
import org.springframework.batch.test.JobLauncherTestUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.DataClassRowMapper
import org.springframework.test.context.jdbc.Sql
import javax.sql.DataSource

@Sql(scripts = ["/db-item-reader/people.sql"])
@SpringBootTest(
    classes = [
        DbItemReaderTest::class,
        DbItemReaderTest.BatchConfig::class
    ],
    properties = [
        "spring.batch.job.enabled=false"
    ]
)
@EnableAutoConfiguration
class DbItemReaderTest(
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
        fun dbJob(@Qualifier("dbStep") dbStep: Step): Job {
            return jobBuilderFactory.get("dbJob")
                .start(dbStep)
                .preventRestart()
                .build()
        }

        @Bean
        fun dbStep(@Qualifier("jdbcCursorItemReader") reader: ItemReader<PeopleModel>): Step {
            return stepBuilderFactory.get("dbStep")
                .chunk<PeopleModel, PeopleModel>(10)
                .reader(reader)
                .processor(PassThroughItemProcessor())
                .writer {
                    println(it)
                    println("Read From DB, length = ${it.size}\n\n")
                }
                .build()
        }

        @Bean
        fun jdbcCursorItemReader(dataSource: DataSource): JdbcCursorItemReader<PeopleModel> {
            return JdbcCursorItemReaderBuilder<PeopleModel>()
                .name("jdbcCursorItemReader")
                .fetchSize(10)
                .dataSource(dataSource)
                .rowMapper(DataClassRowMapper(PeopleModel::class.java))
                .sql("select people_id, first_name, last_name, age, gender, pick from people")
                .build()
        }

        @Bean
        fun jobLauncherTestUtils(): JobLauncherTestUtils {
            return JobLauncherTestUtils()
        }
    }

    class PeopleModel(
        val peopleId: Int,
        val firstName: String,
        val lastName: String,
        val age: String,
        val gender: String,
        val pick: String
    )
}
