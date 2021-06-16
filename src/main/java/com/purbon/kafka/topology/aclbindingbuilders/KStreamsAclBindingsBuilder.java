package com.purbon.kafka.topology.aclbindingbuilders;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.model.users.KStream;

public class KStreamsAclBindingsBuilder implements AclBindingsOrErrorBuilder {

  private final BindingsBuilderProvider builderProvider;
  private final KStream app;
  private final String prefix;

  public KStreamsAclBindingsBuilder(
      BindingsBuilderProvider builderProvider, KStream app, String topicPrefix) {
    this.builderProvider = builderProvider;
    this.app = app;
    this.prefix = app.getApplicationId().orElse(topicPrefix);
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    if (prefix.isEmpty()) {
      return AclBindingsOrError.forError(
          "KStream application prefix should not be empty."
              + " Please define the applicationID or allow a nonEmpty project prefix (aka everything before the topic)");
    }
    return AclBindingsOrError.forAclBindings(
        builderProvider.buildBindingsForStreamsApp(
            app.getPrincipal(),
            prefix,
            app.getTopics().get(KStream.READ_TOPICS),
            app.getTopics().get(KStream.WRITE_TOPICS)));
  }
}
