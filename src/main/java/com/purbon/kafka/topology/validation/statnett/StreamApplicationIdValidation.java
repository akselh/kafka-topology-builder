package com.purbon.kafka.topology.validation.statnett;

import static com.purbon.kafka.topology.validation.statnett.ValidationUtil.reThrowException;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.KStream;
import com.purbon.kafka.topology.validation.TopologyValidation;
import java.util.Optional;

@SuppressWarnings("unused")
public class StreamApplicationIdValidation implements TopologyValidation {

  @Override
  public void valid(Topology topology) throws ValidationException {
    reThrowException(
        () ->
            topology.getProjects().stream()
                .flatMap(p -> p.getStreams().stream())
                .forEach(this::validateApplicationId));
  }

  private void validateApplicationId(KStream stream) throws RuntimeException {
    if (noApplicationId(stream)) {
      throw new RuntimeException(
              String.format("Streams principal '%s' is missing required key applicationId. ", stream.getPrincipal()));
    }
  }

  private boolean noApplicationId(KStream stream) {
    Optional<String> id = stream.getApplicationId();
    return !id.isPresent() || id.get().isEmpty();
  }
}
