package com.purbon.kafka.topology.actions.access.builders;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.model.users.KSqlApp;

public class BuildBindingsForKSqlApp implements AclBindingsOrErrorBuilder {

  private final BindingsBuilderProvider builderProvider;
  private final KSqlApp app;
  private final String prefix;

  public BuildBindingsForKSqlApp(
      BindingsBuilderProvider builderProvider, KSqlApp app, String topicPrefix) {
    this.builderProvider = builderProvider;
    this.app = app;
    this.prefix = app.getApplicationId().orElse(topicPrefix);
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    if (prefix.isEmpty()) {
      return AclBindingsOrError.forError(
          "KSqlApp application prefix should not be empty."
              + " Please define the applicationID or allow a nonEmpty project prefix (aka everything before the topic)");
    }
    return AclBindingsOrError.forAclBindings(builderProvider.buildBindingsForKSqlApp(app, prefix));
  }
}
