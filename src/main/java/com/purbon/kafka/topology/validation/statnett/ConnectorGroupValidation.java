package com.purbon.kafka.topology.validation.statnett;

import static com.purbon.kafka.topology.validation.statnett.ValidationUtil.reThrowException;

import com.purbon.kafka.topology.exceptions.ValidationException;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.Connector;
import com.purbon.kafka.topology.model.users.KStream;
import com.purbon.kafka.topology.validation.TopologyValidation;
import java.util.Optional;

@SuppressWarnings("unused")
public class ConnectorGroupValidation implements TopologyValidation {

  @Override
  public void valid(Topology topology) throws ValidationException {
    reThrowException(
        () ->
            topology.getProjects().stream()
                .flatMap(p -> p.getConnectors().stream())
                .forEach(this::validateGroup));
  }

  private void validateGroup(Connector connector) throws RuntimeException {
    if (connector.getTopics().get(KStream.READ_TOPICS) == null) {
      return; // Not a source connector
    }

    if (noGroup(connector)) {
      throw new RuntimeException(
              String.format("Connectors principal '%s' is missing required key 'group'.", connector.getPrincipal()));
    }
  }

  private boolean noGroup(Connector connector) {
    Optional<String> id = connector.getGroup();
    return !id.isPresent() || id.get().isEmpty();
  }
}
