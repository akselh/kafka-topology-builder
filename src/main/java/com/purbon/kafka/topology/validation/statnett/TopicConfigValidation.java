package com.purbon.kafka.topology.validation.statnett;

import static java.lang.String.format;

import com.purbon.kafka.topology.TopicManager;
import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.validation.TopicValidation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.TopicConfig;

@SuppressWarnings("unused")
public class TopicConfigValidation implements TopicValidation {

  private static final Set<String> legalConfigKeys = new HashSet<>();

  static {
    legalConfigKeys.add(TopicManager.REPLICATION_FACTOR);
    legalConfigKeys.add(TopicManager.NUM_PARTITIONS);

    Field[] fields = TopicConfig.class.getDeclaredFields();
    for (Field f : fields) {
      if (Modifier.isStatic(f.getModifiers()) && f.getName().endsWith("_CONFIG")) {
        try {
          legalConfigKeys.add(String.valueOf(f.get(null)));
        } catch (IllegalAccessException e) {
          // Ignore
        }
      }
    }
  }

  @Override
  public void valid(Topic topic) throws ValidationException {
    Set<String> illegalConfigs = new HashSet<>();
    topic
        .getConfig()
        .keySet()
        .forEach(
            key -> {
              if (isInvalidConfigKey(key)) {
                illegalConfigs.add(key);
              }
            });

    if (!illegalConfigs.isEmpty()) {
      String configs = StringUtils.join(illegalConfigs, ", ");
      throw new ValidationException(format("Topic '%s' has invalid config(s). " +
              "Please check the Kafka documentation on topic configuration. Invalid configs: %s",
              topic.getName(), configs));
    }
  }

  private boolean isInvalidConfigKey(String configKey) {
    return !legalConfigKeys.contains(configKey);
  }
}
