package com.purbon.kafka.topology.aclbindingbuilders.rbac;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.aclbindingbuilders.AclBindingsOrError;
import com.purbon.kafka.topology.aclbindingbuilders.AclBindingsOrErrorBuilder;
import com.purbon.kafka.topology.model.users.Connector;
import java.util.ArrayList;

public class ConnectorAuthorizationAclBindingsBuilder implements AclBindingsOrErrorBuilder {

  private final Connector connector;
  private final BindingsBuilderProvider builderProvider;

  public ConnectorAuthorizationAclBindingsBuilder(
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
