package com.max.driver;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine;

@Slf4j
@Component
@RequiredArgsConstructor(onConstructor_ = {@Autowired})
@CommandLine.Command(name = "sudoku")
public final class SudokuDriver {


    Logger LOGGER = LoggerFactory.getLogger(SudokuDriver.class);

    @CommandLine.Command(name = "subjects")
    public void runSubjects() {
        LOGGER.info("I am here");
    }
}
