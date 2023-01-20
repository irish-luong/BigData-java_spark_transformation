package com.max.config.properties;


import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ConfigurationProperties(prefix = "application")
public final class ApplicationProperties implements Serializable {

    Map<String, String> sparkConf;

    DerbyProperties derby;
}
