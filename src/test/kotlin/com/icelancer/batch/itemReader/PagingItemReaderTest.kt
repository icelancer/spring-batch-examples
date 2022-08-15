package com.icelancer.batch.itemReader

import com.icelancer.batch.itemReader.entity.PeopleEntity
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.database.JdbcPagingItemReader
import org.springframework.batch.item.database.JpaPagingItemReader
import org.springframework.batch.item.database.PagingQueryProvider
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean
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
        PagingItemReaderTest::class,
        PagingItemReaderTest.BatchConfig::class
    ],
    properties = [
        "spring.batch.job.enabled=false",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.jpa.show-sql=true",
        "logging.level.org.springframework.batch=INFO",
        "logging.level.org.springframework.jdbc.core.JdbcTemplate=DEBUG",
        "logging.level.org.springframework.jdbc.core=TRACE"
    ]
)
@EnableAutoConfiguration
class PagingItemReaderTest(
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
        fun job(
            @Qualifier("jdbcPagingStep") jdbcPagingStep: Step,
            @Qualifier("jpaPagingStep") jpaPagingStep: Step

        ): Job {
            return jobBuilderFactory.get("pagingJob")
                .start(jdbcPagingStep)
                .next(jpaPagingStep)
                .build()
        }

        @Bean
        fun jdbcPagingStep(
            @Qualifier("jdbcPagingItemReader")
            reader: ItemReader<PeopleEntity>
        ): Step {
            return createStep("jdbcPagingStep", reader)
        }

        @Bean
        fun jpaPagingStep(
            @Qualifier("jpaPagingItemReader")
            reader: ItemReader<PeopleEntity>
        ): Step {
            return createStep("jpaPagingStep", reader)
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
        fun jpaPagingItemReader(
            entityManagerFactory: EntityManagerFactory
        ): JpaPagingItemReader<PeopleEntity> {
            val parameterValues = mapOf("pick" to "RED")

            return JpaPagingItemReaderBuilder<PeopleEntity>()
                .name("jpaPagingItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select p from PeopleEntity p where p.pick = :pick")
                .parameterValues(parameterValues)
                .pageSize(10)
                .build()
        }

        @Bean
        fun jdbcPagingItemReader(
            dataSource: DataSource,
            provider: PagingQueryProvider
        ): JdbcPagingItemReader<PeopleEntity> {
            val parameterValues = mapOf("pick" to "RED")

            return JdbcPagingItemReaderBuilder<PeopleEntity>()
                .name("jdbcPagingItemReader")
                .dataSource(dataSource)
                .queryProvider(provider)
                .parameterValues(parameterValues)
                .rowMapper(DataClassRowMapper(PeopleEntity::class.java))
                .pageSize(10)
                .build()
        }

        @Bean
        fun jdbcPagingQueryProvider(
            dataSource: DataSource
        ): SqlPagingQueryProviderFactoryBean {
            return SqlPagingQueryProviderFactoryBean().apply {
                this.setSelectClause("select people_id, first_name, last_name, age, gender, pick")
                this.setFromClause("from people")
                this.setWhereClause("where pick=:pick")
                this.setSortKey("people_id")
                this.setDataSource(dataSource)
            }
        }

        @Bean
        fun jobLauncherTestUtils(): JobLauncherTestUtils {
            return JobLauncherTestUtils()
        }
    }
}
