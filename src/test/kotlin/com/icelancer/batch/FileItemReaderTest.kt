package com.icelancer.batch

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.springframework.batch.core.BatchStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.item.json.GsonJsonObjectReader
import org.springframework.batch.item.json.JacksonJsonObjectReader
import org.springframework.batch.item.json.JsonItemReader
import org.springframework.batch.item.json.JsonObjectReader
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder
import org.springframework.batch.item.support.PassThroughItemProcessor
import org.springframework.batch.test.JobLauncherTestUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.core.io.ClassPathResource

@SpringBootTest(classes = [FileItemReaderTest::class, FileItemReaderTest.BatchConfig::class])
class FileItemReaderTest(
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
        fun fileJob(@Qualifier("fileStep") fileStep: Step): Job {
            return jobBuilderFactory.get("fileJob")
                .start(fileStep)
                .build()
        }

        @Bean
        fun fileStep(@Qualifier("csvReader") reader: ItemReader<Input>): Step {
            return stepBuilderFactory.get("fileStep")
                .chunk<Input, Input>(10)
                .reader(reader)
                .processor(PassThroughItemProcessor())
                .writer {
                    println(it)
                    println("Read From File, length = ${it.size}\n\n")
                }
                .build()
        }

        @Bean
        fun csvReader(): FlatFileItemReader<Input> {
            return FlatFileItemReaderBuilder<Input>()
                .name("csvItemReader")
                .resource(ClassPathResource("file-item-reader/file-batch-persons.csv"))
                .delimited()
                .names(*arrayOf("name", "age", "state", "zip", "pick", "phone", "gender"))
                .fieldSetMapper { fieldSet ->
                    Input(
                        name = fieldSet.readString("name"),
                        age = fieldSet.readInt("age"),
                        state = fieldSet.readString("state"),
                        zip = fieldSet.readString("zip"),
                        pick = fieldSet.readString("pick"),
                        phone = fieldSet.readString("phone"),
                        gender = fieldSet.readString("gender")
                    )
                }
                .build()
        }

        @Bean
        fun jsonReader(jsonReader: JsonObjectReader<Input>): JsonItemReader<Input> {
            return JsonItemReaderBuilder<Input>()
                .name("jsonItemReader")
                .resource(ClassPathResource("file-item-reader/file-batch-persons.json"))
                .jsonObjectReader(jsonReader)
                .build()
        }

        @Bean
        fun jacksonInputReader(): JsonObjectReader<Input> {
            return JacksonJsonObjectReader(
                ObjectMapper().registerKotlinModule(),
                Input::class.java
            )
        }

        @Bean
        @Primary
        fun gsonInputReader(): JsonObjectReader<Input> {
            return GsonJsonObjectReader(Input::class.java)
        }

        @Bean
        fun jobLauncherTestUtils(): JobLauncherTestUtils {
            return JobLauncherTestUtils()
        }
    }

    data class Input(
        val name: String,
        val age: Int,
        val state: String,
        val zip: String,
        val pick: String,
        val phone: String,
        val gender: String
    )
}
