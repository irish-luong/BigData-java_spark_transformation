package com.max;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.spring.PicocliSpringFactory;

import java.util.concurrent.atomic.AtomicInteger;


@Slf4j
@Component
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
public class ApplicationRunner implements
        CommandLineRunner,
        ExitCodeGenerator {

    private final CommandSpec commandSpec;
    private final PicocliSpringFactory factory;

    private final AtomicInteger exitCode = new AtomicInteger(0);


    @Override
    public void run(String... args) {
        exitCode.set(new CommandLine(commandSpec, factory)
                        .execute(args));
    }

    @Override
    public int getExitCode() {
        return exitCode.get();
    }
}
