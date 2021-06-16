package com.purbon.kafka.topology.aclbindingbuilders;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.model.users.Connector;

public class KConnectAclBindingsBuilder implements AclBindingsOrErrorBuilder {

  private final Connector app;
  private final String topicPrefix;
  private final BindingsBuilderProvider controlProvider;

  public KConnectAclBindingsBuilder(
      BindingsBuilderProvider controlProvider, Connector app, String topicPrefix) {
    this.app = app;
    this.topicPrefix = topicPrefix;
    this.controlProvider = controlProvider;
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    return AclBindingsOrError.forAclBindings(
        controlProvider.buildBindingsForConnect(app, topicPrefix));
  }
}
