package com.purbon.kafka.topology.actions.access.builders;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.model.users.Consumer;
import java.util.List;

public class BuildBindingsForConsumer implements AclBindingsOrErrorBuilder {

  private final String fullTopicName;
  private final List<Consumer> consumers;
  private final BindingsBuilderProvider builderProvider;
  private boolean prefixed;

  public BuildBindingsForConsumer(
      BindingsBuilderProvider builderProvider,
      List<Consumer> consumers,
      String fullTopicName,
      boolean prefixed) {
    this.consumers = consumers;
    this.fullTopicName = fullTopicName;
    this.builderProvider = builderProvider;
    this.prefixed = prefixed;
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    return AclBindingsOrError.forAclBindings(
        builderProvider.buildBindingsForConsumers(consumers, fullTopicName, prefixed));
  }
}
