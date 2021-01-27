package com.purbon.kafka.topology.validation.statnett;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.validation.TopicValidation;

import static java.lang.String.format;

@SuppressWarnings("unused")
public class TopicNameValidation implements TopicValidation {

  @Override
  public void valid(Topic topic) throws ValidationException {
    if (topic.getName().contains("-")) {
      throw new ValidationException(format("Invalid topic name '%s'. Only alphanumeric characters and '_' are allowed.",
              topic.getName()));
    }
  }
}
