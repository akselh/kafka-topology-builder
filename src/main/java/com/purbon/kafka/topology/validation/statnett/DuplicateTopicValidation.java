package com.purbon.kafka.topology.validation.statnett;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.validation.TopologyValidation;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

@SuppressWarnings("unused")
public class DuplicateTopicValidation implements TopologyValidation {

  @Override
  public void valid(Topology topology) throws ValidationException {
    List<String> topicNames =
        topology.getProjects().stream()
            .flatMap(p -> p.getTopics().stream())
            .map(Topic::getName)
            .collect(Collectors.toList());

    Set<String> duplicates = findDuplicates(topicNames);
    if (!duplicates.isEmpty()) {
      String duplicateTopicNames = StringUtils.join(duplicates, ", ");
      throw new ValidationException("Topic(s) already exists in topology: " + duplicateTopicNames);
    }
  }

  private <T> Set<T> findDuplicates(Collection<T> collection) {
    Set<T> duplicates = new LinkedHashSet<>();
    Set<T> copy = new HashSet<>();

    for (T t : collection) {
      if (!copy.add(t)) {
        duplicates.add(t);
      }
    }

    return duplicates;
  }
}
