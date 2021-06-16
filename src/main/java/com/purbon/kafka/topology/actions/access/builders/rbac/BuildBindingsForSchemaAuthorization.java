package com.purbon.kafka.topology.actions.access.builders.rbac;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.actions.access.builders.AclBindingsOrError;
import com.purbon.kafka.topology.actions.access.builders.AclBindingsOrErrorBuilder;
import com.purbon.kafka.topology.model.users.Schemas;

public class BuildBindingsForSchemaAuthorization implements AclBindingsOrErrorBuilder {

  private final BindingsBuilderProvider builderProvider;
  private final Schemas schemaAuthorization;

  public BuildBindingsForSchemaAuthorization(
      BindingsBuilderProvider builderProvider, Schemas schemaAuthorization) {
    this.builderProvider = builderProvider;
    this.schemaAuthorization = schemaAuthorization;
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    return AclBindingsOrError.forAclBindings(
        builderProvider.setSchemaAuthorization(
            schemaAuthorization.getPrincipal(), schemaAuthorization.getSubjects()));
  }
}
