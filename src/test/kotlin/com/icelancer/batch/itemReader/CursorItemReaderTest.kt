package com.icelancer.batch.itemReader

import com.icelancer.batch.itemReader.entity.PeopleEntity
import org.hibernate.SessionFactory
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.database.HibernateCursorItemReader
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.batch.item.database.builder.HibernateCursorItemReaderBuilder
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
import javax.persistence.EntityManagerFactory
import javax.sql.DataSource

@Sql(scripts = ["/db-item-reader/people.sql"])
@SpringBootTest(
    classes = [
        CursorItemReaderTest::class,
        CursorItemReaderTest.BatchConfig::class
    ],
    properties = [
        "spring.batch.job.enabled=false",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.jpa.show-sql=true",
        "logging.level.org.springframework.batch=INFO"
    ]
)
@EnableAutoConfiguration
class CursorItemReaderTest(
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
        fun dbJob(
            @Qualifier("hibernateStep") hibernateStep: Step,
            @Qualifier("jdbcStep") jdbcStep: Step
        ): Job {
            return jobBuilderFactory.get("dbJob")
                .start(hibernateStep)
                .next(jdbcStep)
                .preventRestart()
                .build()
        }

        @Bean
        fun jdbcStep(
            @Qualifier("jdbcCursorItemReader")
            reader: ItemReader<PeopleEntity>
        ): Step {
            return createStep("jdbcStep", reader)
        }

        @Bean
        fun hibernateStep(
            @Qualifier("hibernateCursorItemReader")
            reader: ItemReader<PeopleEntity>
        ): Step {
            return createStep("hibernateStep", reader)
        }

        fun createStep(name: String, reader: ItemReader<PeopleEntity>): Step {
            return stepBuilderFactory.get(name)
                .chunk<PeopleEntity, PeopleEntity>(10)
                .reader(reader)
                .processor(PassThroughItemProcessor())
                .writer {
                    println(it)
                    println("Read From DB, length = ${it.size}\n\n")
                }
                .build()
        }

        @Bean
        fun jdbcCursorItemReader(dataSource: DataSource): JdbcCursorItemReader<PeopleEntity> {
            return JdbcCursorItemReaderBuilder<PeopleEntity>()
                .name("jdbcCursorItemReader")
                .fetchSize(10)
                .dataSource(dataSource)
                .rowMapper(DataClassRowMapper(PeopleEntity::class.java))
                .sql("select people_id, first_name, last_name, age, gender, pick from people")
                .build()
        }

        @Bean
        fun hibernateCursorItemReader(entityManagerFactory: EntityManagerFactory): HibernateCursorItemReader<PeopleEntity> {
            val sessionFactory = entityManagerFactory.unwrap(SessionFactory::class.java)

            return HibernateCursorItemReaderBuilder<PeopleEntity>()
                .name("hibernateCursorItemReader")
                .sessionFactory(sessionFactory)
                .useStatelessSession(true)
                .queryString("from PeopleEntity")
                .build()
        }

        @Bean
        fun jobLauncherTestUtils(): JobLauncherTestUtils {
            return JobLauncherTestUtils()
        }
    }
}
