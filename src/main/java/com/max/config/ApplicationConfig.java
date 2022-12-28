package com.max.config;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.ComponentScan;


@Configuration
@ConfigurationPropertiesScan
@ComponentScan(
        basePackages = "com.max.driver",
        includeFilters = @ComponentScan.Filter(CommandLine.Command.class)
)
public class ApplicationConfig {


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
