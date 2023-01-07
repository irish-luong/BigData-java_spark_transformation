package com.max.config.properties;


import lombok.Builder;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Data
@Builder
@ConfigurationProperties(prefix = "application")
public class ApplicationProperties {

    Map<String, String> sparkConf;
}
