package com.max.config;

import com.max.config.properties.ApplicationProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.ComponentScan;

import java.util.Optional;


@Configuration
@ConfigurationPropertiesScan
@ComponentScan(
        basePackages = "com.max.driver",
        includeFilters = @ComponentScan.Filter(CommandLine.Command.class)
)
public class ApplicationConfig {


    /**
     *
     * @return spark configuration objects
     */
    @Bean
    public SparkConf sparkConf(ApplicationProperties applicationProperties) {


        SparkConf conf = new SparkConf();

        // Scan through Spark configuration properties
        // Create config by key-value pair in properties
        Optional.ofNullable(applicationProperties)
                .map(ApplicationProperties::getSparkConf)
                .ifPresent(x -> x.forEach(conf::set));

        return conf;
    }

    /**
     * Load a spark session and ship to components in Spring application.
     *
     * @param sparkConf bean spark configuration
     * @return a spark session
     */
    @Bean
    public SparkSession sparkSession(SparkConf sparkConf) {

        return SparkSession.builder().config(sparkConf).getOrCreate();
    }


    /**
     * Load command lines into application context
     *
     * @param applicationContext Spring Boot application context
     * @return command spec
     */
    @Bean
    public CommandSpec commandSpec(ApplicationContext applicationContext) {
        CommandSpec commandSpec = CommandSpec.create();

        applicationContext.getBeansWithAnnotation(CommandLine.Command.class)
                .values()
                .forEach(cmd -> {
                    String name = cmd.getClass().getAnnotation(CommandLine.Command.class).name();
                    commandSpec.addSubcommand(name, CommandSpec.forAnnotatedObject(cmd));
                });

        return commandSpec;
    }

}
