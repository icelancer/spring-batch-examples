package com.icelancer.batch

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
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.DataClassRowMapper
import org.springframework.test.context.jdbc.Sql
import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.EntityManagerFactory
import javax.persistence.Id
import javax.persistence.Table
import javax.sql.DataSource

@Sql(scripts = ["/db-item-reader/people.sql"])
@SpringBootTest(
    classes = [
        DbItemReaderTest::class,
        DbItemReaderTest.BatchConfig::class,
        SpringBatchExamplesApplication::class
    ],
    properties = [
        "spring.batch.job.enabled=false",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.jpa.show-sql=true",
        "logging.level.org.springframework.batch=INFO"
    ]
)
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
        fun dbStep(@Qualifier("jdbcCursorItemReader") reader: ItemReader<People>): Step {
            return stepBuilderFactory.get("dbStep")
                .chunk<People, People>(10)
                .reader(reader)
                .processor(PassThroughItemProcessor())
                .writer {
                    println(it)
                    println("Read From DB, length = ${it.size}\n\n")
                }
                .build()
        }

        @Bean
        fun jdbcCursorItemReader(dataSource: DataSource): JdbcCursorItemReader<People> {
            return JdbcCursorItemReaderBuilder<People>()
                .name("jdbcCursorItemReader")
                .fetchSize(10)
                .dataSource(dataSource)
                .rowMapper(DataClassRowMapper(People::class.java))
                .sql("select people_id, first_name, last_name, age, gender, pick from people")
                .build()
        }

        @Bean
        fun hibernateCursorItemReader(entityManagerFactory: EntityManagerFactory): HibernateCursorItemReader<People> {
            val sessionFactory = entityManagerFactory.unwrap(SessionFactory::class.java)

            return HibernateCursorItemReaderBuilder<People>()
                .name("hibernateCursorItemReader")
                .sessionFactory(sessionFactory)
                .queryString("from PeopleModel")
                .build()
        }

        @Bean
        fun jobLauncherTestUtils(): JobLauncherTestUtils {
            return JobLauncherTestUtils()
        }
    }
}

@Entity
@Table(name = "people")
data class People(
    @Id
    @Column(name = "people_id")
    val peopleId: Int,
    val firstName: String,
    val lastName: String,
    val age: String,
    val gender: String,
    val pick: String
)
