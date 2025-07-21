package org.example.config;

import org.example.model.AppConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;import org.yaml.snakeyaml.Yaml;

public class Configuration {

    public AppConfig load() throws IOException {
        Yaml yaml = new Yaml();
        return yaml.loadAs(extractYamlValuesAsString("application-"+System.getenv("ENVIRONMENT")+".yaml"), AppConfig.class);
    }

    private String extractYamlValuesAsString(String yamlFile) throws IOException {
        String rawYaml = new String(Objects.requireNonNull(Configuration.class.getClassLoader().getResourceAsStream(yamlFile)).readAllBytes(), StandardCharsets.UTF_8);
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            rawYaml = rawYaml.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return rawYaml;
    }
}
