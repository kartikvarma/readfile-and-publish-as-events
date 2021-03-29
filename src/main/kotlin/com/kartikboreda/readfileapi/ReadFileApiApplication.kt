package com.kartikboreda.readfileapi

import com.fasterxml.jackson.databind.ser.std.StringSerializer
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder
import org.springframework.batch.item.kafka.KafkaItemWriter
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

@EnableBatchProcessing
@SpringBootApplication
class ReadFileApiApplication

fun main(args: Array<String>) {
    runApplication<ReadFileApiApplication>(*args)
}


@Configuration
@EnableKafka
class KafkaConfig {

    @Bean
    fun producerConfigs(): Map<String, Any> {
        val props = HashMap<String, Any>()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        return props
    }

    @Bean
    fun producerFactory(): ProducerFactory<String, String> {
        return DefaultKafkaProducerFactory(producerConfigs())
    }

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, String> {
        return KafkaTemplate(producerFactory())
    }

}


@Configuration()
class JobConfiguration(
    private val jobBuilderFactory: JobBuilderFactory,
    private val stepBuilderFactory: StepBuilderFactory,
    private val kafkaTemplate: KafkaTemplate<String, String>
) {


    @Bean
    fun readFileAndPublishJob(): Job {
        return jobBuilderFactory.get("readFileAndPublishJob")
            .start(readFileAndPublishStep())
            .build()

    }

    @Bean
    fun readFileAndPublishStep(): Step {
        return stepBuilderFactory.get("readFileAndPublishStep")
            .chunk<String, String>(100)
            .reader(fileItemReader())
            .writer(kafkaItemWriter())
            .build()
    }

    @Bean
    fun fileItemReader(): FlatFileItemReader<String> {
        return FlatFileItemReaderBuilder<String>()
            .name("flatFileItemReader")
            .build()
    }

    @Bean
    fun kafkaItemWriter(): KafkaItemWriter<String, String> {
        return KafkaItemWriterBuilder<String, String>()
            .kafkaTemplate(kafkaTemplate)
            .itemKeyMapper { toString() }
            .build()
    }

}