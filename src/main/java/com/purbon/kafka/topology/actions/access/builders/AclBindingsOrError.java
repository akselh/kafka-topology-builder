package com.purbon.kafka.topology.actions.access.builders;

import com.purbon.kafka.topology.roles.TopologyAclBinding;
import java.util.Collection;

public final class AclBindingsOrError {

  private Collection<TopologyAclBinding> aclBindings;
  private String errorMessage;

  private AclBindingsOrError(Collection<TopologyAclBinding> aclBindings, String errorMessage) {
    this.aclBindings = aclBindings;
    this.errorMessage = errorMessage;
  }

  public static AclBindingsOrError forError(String errorMessage) {
    return new AclBindingsOrError(null, errorMessage);
  }

  public static AclBindingsOrError forAclBindings(Collection<TopologyAclBinding> aclBindings) {
    return new AclBindingsOrError(aclBindings, null);
  }

  public boolean isError() {
    return errorMessage != null;
  }

  public Collection<TopologyAclBinding> getAclBindings() {
    return aclBindings;
  }

  public String getErrorMessage() {
    return errorMessage;
  }
}
