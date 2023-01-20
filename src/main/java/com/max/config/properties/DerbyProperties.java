package com.max.config.properties;


import lombok.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Properties;

@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ConfigurationProperties(prefix = "application.derby")
public class DerbyProperties {

    String url;
    String user;
    String password;


    public Properties getDerbyProperties () {
        Properties derbyProperties = new Properties();
        derbyProperties.put("user", user);
        derbyProperties.put("password", password);
        return derbyProperties;
    }

}
