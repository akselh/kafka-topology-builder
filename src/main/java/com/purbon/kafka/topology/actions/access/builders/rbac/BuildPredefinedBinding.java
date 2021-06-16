package com.purbon.kafka.topology.actions.access.builders.rbac;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.actions.access.builders.AclBindingsOrError;
import com.purbon.kafka.topology.actions.access.builders.AclBindingsOrErrorBuilder;
import java.util.Collections;

public class BuildPredefinedBinding implements AclBindingsOrErrorBuilder {

  private final BindingsBuilderProvider builderProvider;
  private final String principal;
  private final String predefinedRole;
  private final String topicPrefix;

  public BuildPredefinedBinding(
      BindingsBuilderProvider builderProvider,
      String principal,
      String predefinedRole,
      String topicPrefix) {
    this.builderProvider = builderProvider;
    this.principal = principal;
    this.predefinedRole = predefinedRole;
    this.topicPrefix = topicPrefix;
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    return AclBindingsOrError.forAclBindings(
        Collections.singletonList(
            builderProvider.setPredefinedRole(principal, predefinedRole, topicPrefix)));
  }
}
