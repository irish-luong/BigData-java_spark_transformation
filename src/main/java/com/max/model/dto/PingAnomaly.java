package com.max.model.dto;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
@Data
public class PingAnomaly implements Serializable {

    private String source;
    private String destination;

    public PingAnomaly(String source, String destination) {
        this.source = source;
        this.destination = destination;
    }

}
