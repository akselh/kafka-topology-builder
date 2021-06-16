package com.purbon.kafka.topology.actions.access.builders.rbac;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.actions.access.builders.AclBindingsOrError;
import com.purbon.kafka.topology.actions.access.builders.AclBindingsOrErrorBuilder;
import com.purbon.kafka.topology.model.users.Connector;
import java.util.ArrayList;

public class BuildBindingsForConnectorAuthorization implements AclBindingsOrErrorBuilder {

  private final Connector connector;
  private final BindingsBuilderProvider builderProvider;

  public BuildBindingsForConnectorAuthorization(
      BindingsBuilderProvider builderProvider, Connector connector) {
    this.builderProvider = builderProvider;
    this.connector = connector;
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    return AclBindingsOrError.forAclBindings(
        builderProvider.setConnectorAuthorization(
            connector.getPrincipal(), connector.getConnectors().orElse(new ArrayList<>())));
  }
}
