package com.ibm.test.kfkconn.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.List;

public class ReplaceStringFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Replace multiple string patterns in a Kafka message using regular expressions.";

    private static final String PATTERNS_CONFIG = "patterns";
    private static final String REPLACEMENT_CONFIG = "replacement";
    private static final String FIELD_CONFIG = "field";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PATTERNS_CONFIG, Type.STRING, Importance.HIGH, "Comma-separated list of regex patterns to match.")
            .define(REPLACEMENT_CONFIG, Type.STRING, Importance.HIGH, "The replacement string for matched patterns.")
            .define(FIELD_CONFIG, Type.STRING, Importance.HIGH, "The field to apply the transformation on.");

    private static final Logger log = LoggerFactory.getLogger(ReplaceStringFieldValue.class);

    private List<Pattern> patternList;
    private String replacement;
    private String field;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        field = config.getString(FIELD_CONFIG);
        replacement = config.getString(REPLACEMENT_CONFIG);

        patternList = Pattern.compile(",")
                .splitAsStream(config.getString(PATTERNS_CONFIG))
                .map(Pattern::compile)
                .collect(Collectors.toList());

        if (patternList.isEmpty()) {
            throw new ConfigException(PATTERNS_CONFIG, config.getString(PATTERNS_CONFIG), "Must contain at least one pattern");
        }

        log.info("Configured RegexpReplace SMT with patterns: {} and replacement: {}", patternList, replacement);
    }

    @SuppressWarnings("unchecked")
    @Override
    public R apply(R record) {
        Object value = record.value();

        if (value instanceof Map) {
            log.info("Value is a Map for field {}", field);
            Map<String, Object> valueMap = (Map<String, Object>) value;
            if (valueMap.containsKey(field)) {
                String fieldValue = (String) valueMap.get(field);
                String transformedValue = replacePatterns(fieldValue);
                log.info("Original value: {}, transformed value: {}", fieldValue, transformedValue);
                valueMap.put(field, transformedValue);
            }
        } else if (value instanceof String) { // && field.equalsIgnoreCase("value")
            log.info("Value is a String:");
            String fieldValue = (String) value;
            String transformedValue = replacePatterns(fieldValue);
            log.info("Original value: {}, transformed value: {}", fieldValue, transformedValue);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), transformedValue, record.timestamp());
        }

        return record;
    }

    private String replacePatterns(String value) {
        for (Pattern pattern : patternList) {
            value = pattern.matcher(value).replaceAll(replacement);
        }
        return value;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No resources to close
    }
    /*
    @Override
    public void reset() {
        // Reset any instance-specific state
    }
    */
}
