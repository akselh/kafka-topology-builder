package com.purbon.kafka.topology.aclbindingbuilders;

import com.purbon.kafka.topology.BindingsBuilderProvider;
import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistryInstance;

public class SchemaRegistryAclBindingsBuilder implements AclBindingsOrErrorBuilder {

  private final BindingsBuilderProvider builderProvider;
  private final SchemaRegistryInstance schemaRegistry;

  public SchemaRegistryAclBindingsBuilder(
      BindingsBuilderProvider builderProvider, SchemaRegistryInstance schemaRegistry) {
    this.builderProvider = builderProvider;
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public AclBindingsOrError getAclBindingsOrError() {
    try {
      return AclBindingsOrError.forAclBindings(
          builderProvider.buildBindingsForSchemaRegistry(schemaRegistry));
    } catch (ConfigurationException e) {
      return AclBindingsOrError.forError(e.getMessage());
    }
  }
}
