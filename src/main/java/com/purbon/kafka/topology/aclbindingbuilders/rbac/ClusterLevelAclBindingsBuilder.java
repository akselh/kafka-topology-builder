package com.purbon.kafka.topology.aclbindingbuilders.rbac;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.aclbindingbuilders.AclBindingsOrError;
import com.purbon.kafka.topology.aclbindingbuilders.AclBindingsOrErrorBuilder;
import com.purbon.kafka.topology.model.Component;
import com.purbon.kafka.topology.model.User;
import java.io.IOException;

public class ClusterLevelAclBindingsBuilder implements AclBindingsOrErrorBuilder {

  private final String role;
  private final User user;
  private final Component cmp;
  private final BindingsBuilderProvider builderProvider;

  public ClusterLevelAclBindingsBuilder(
      BindingsBuilderProvider builderProvider, String role, User user, Component cmp) {
    this.builderProvider = builderProvider;
    this.role = role;
    this.user = user;
    this.cmp = cmp;
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    try {
      return AclBindingsOrError.forAclBindings(
          builderProvider.setClusterLevelRole(role, user.getPrincipal(), cmp));
    } catch (IOException e) {
      return AclBindingsOrError.forError(e.getMessage());
    }
  }
}
