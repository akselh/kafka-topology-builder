package com.purbon.kafka.topology.actions.access.builders;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.model.users.platform.KsqlServerInstance;

public class BuildBindingsForKSqlServer implements AclBindingsOrErrorBuilder {

  private final BindingsBuilderProvider builderProvider;
  private final KsqlServerInstance ksqlServer;

  public BuildBindingsForKSqlServer(
      BindingsBuilderProvider builderProvider, KsqlServerInstance ksqlServer) {
    this.builderProvider = builderProvider;
    this.ksqlServer = ksqlServer;
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    return AclBindingsOrError.forAclBindings(
        builderProvider.buildBindingsForKSqlServer(ksqlServer));
  }
}
