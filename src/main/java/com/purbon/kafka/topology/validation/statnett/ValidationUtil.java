package com.purbon.kafka.topology.validation.statnett;

import com.purbon.kafka.topology.exceptions.ValidationException;

public class ValidationUtil {
  public static void reThrowException(Runnable function) throws ValidationException {
    try {
      function.run();
    } catch (RuntimeException e) {
      throw new ValidationException(e.getMessage());
    }
  }
}
