package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.model.Component.*;

import com.purbon.kafka.topology.aclbindingbuilders.*;
import com.purbon.kafka.topology.aclbindingbuilders.rbac.*;
import com.purbon.kafka.topology.actions.Action;
import com.purbon.kafka.topology.actions.access.ClearBindings;
import com.purbon.kafka.topology.actions.access.CreateBindings;
import com.purbon.kafka.topology.model.Component;
import com.purbon.kafka.topology.model.DynamicUser;
import com.purbon.kafka.topology.model.Platform;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.User;
import com.purbon.kafka.topology.model.users.Connector;
import com.purbon.kafka.topology.model.users.Consumer;
import com.purbon.kafka.topology.model.users.KSqlApp;
import com.purbon.kafka.topology.model.users.KStream;
import com.purbon.kafka.topology.model.users.Producer;
import com.purbon.kafka.topology.model.users.Schemas;
import com.purbon.kafka.topology.model.users.platform.ControlCenterInstance;
import com.purbon.kafka.topology.model.users.platform.KsqlServerInstance;
import com.purbon.kafka.topology.model.users.platform.SchemaRegistryInstance;
import com.purbon.kafka.topology.roles.TopologyAclBinding;
import com.purbon.kafka.topology.utils.Either;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AccessControlManager implements ExecutionPlanUpdater {

  private static final Logger LOGGER = LogManager.getLogger(AccessControlManager.class);

  private final Configuration config;
  private AccessControlProvider controlProvider;
  private BindingsBuilderProvider bindingsBuilder;
  private final List<String> managedServiceAccountPrefixes;
  private final List<String> managedTopicPrefixes;
  private final List<String> managedGroupPrefixes;

  public AccessControlManager(
      AccessControlProvider controlProvider, BindingsBuilderProvider builderProvider) {
    this(controlProvider, builderProvider, new Configuration());
  }

  public AccessControlManager(
      AccessControlProvider controlProvider,
      BindingsBuilderProvider builderProvider,
      Configuration config) {
    this.controlProvider = controlProvider;
    this.bindingsBuilder = builderProvider;
    this.config = config;
    this.managedServiceAccountPrefixes = config.getServiceAccountManagedPrefixes();
    this.managedTopicPrefixes = config.getTopicManagedPrefixes();
    this.managedGroupPrefixes = config.getGroupManagedPrefixes();
  }

  /**
   * Main apply method, append to the execution plan the necessary bindings to update the access
   * control
   *
   * @param plan An Execution plan
   * @param topology A topology file descriptor
   */
  @Override
  public void updatePlan(ExecutionPlan plan, final Topology topology) throws IOException {
    List<AclBindingsOrError> aclBindingsOrErrors = buildProjectAclBindings(topology);
    aclBindingsOrErrors.addAll(buildPlatformLevelActions(topology));
    buildUpdateBindingsActions(aclBindingsOrErrors, loadActualClusterStateIfAvailable(plan))
        .forEach(plan::add);
  }

  private Set<TopologyAclBinding> loadActualClusterStateIfAvailable(ExecutionPlan plan) {
    Set<TopologyAclBinding> bindings =
        config.fetchStateFromTheCluster() ? providerBindings() : plan.getBindings();
    return bindings.stream()
        .filter(this::matchesManagedPrefixList)
        .filter(this::isNotInternalAcl)
        .collect(Collectors.toSet());
  }

  private boolean isNotInternalAcl(TopologyAclBinding binding) {
    Optional<String> internalPrincipal = config.getInternalPrincipalOptional();
    return internalPrincipal.map(i -> !binding.getPrincipal().equals(i)).orElse(true);
  }

  private Set<TopologyAclBinding> providerBindings() {
    Set<TopologyAclBinding> bindings = new HashSet<>();
    controlProvider.listAcls().values().forEach(bindings::addAll);
    return bindings;
  }

  /**
   * Build the core list of actions builders for creating access control rules
   *
   * @param topology A topology file
   * @return List<Action> A list of actions required based on the parameters
   */
  private List<AclBindingsOrError> buildProjectAclBindings(Topology topology) {
    List<AclBindingsOrError> aclBindingsOrErrors = new ArrayList<>();

    for (Project project : topology.getProjects()) {
      if (config.shouldOptimizeAcls()) {
        aclBindingsOrErrors.addAll(buildOptimizeConsumerAndProducerAcls(project));
      } else {
        aclBindingsOrErrors.addAll(buildDetailedConsumerAndProducerAcls(project));
      }
      // Setup global Kafka Stream Access control lists
      String topicPrefix = project.namePrefix();
      for (KStream app : project.getStreams()) {
        syncApplicationAcls(app, topicPrefix).ifPresent(aclBindingsOrErrors::add);
      }
      for (KSqlApp kSqlApp : project.getKSqls()) {
        syncApplicationAcls(kSqlApp, topicPrefix).ifPresent(aclBindingsOrErrors::add);
      }
      for (Connector connector : project.getConnectors()) {
        syncApplicationAcls(connector, topicPrefix).ifPresent(aclBindingsOrErrors::add);
        connector
            .getConnectors()
            .ifPresent(
                (list) -> {
                  aclBindingsOrErrors.add(
                      new ConnectorAuthorizationAclBindingsBuilder(bindingsBuilder, connector)
                          .getAclBindingsOrError());
                });
      }

      for (Schemas schemaAuthorization : project.getSchemas()) {
        aclBindingsOrErrors.add(
            new SchemaAuthorizationAclBindingsBuilder(bindingsBuilder, schemaAuthorization)
                .getAclBindingsOrError());
      }

      syncRbacRawRoles(project.getRbacRawRoles(), topicPrefix, aclBindingsOrErrors);
    }
    return aclBindingsOrErrors;
  }

  private List<AclBindingsOrError> buildOptimizeConsumerAndProducerAcls(Project project) {
    List<AclBindingsOrError> aclBindingsOrErrors = new ArrayList<>();
    aclBindingsOrErrors.add(
        new ConsumerAclBindingsBuilder(
                bindingsBuilder, project.getConsumers(), project.namePrefix(), true)
            .getAclBindingsOrError());
    aclBindingsOrErrors.add(
        new ProducerAclBindingsBuilder(
                bindingsBuilder, project.getProducers(), project.namePrefix(), true)
            .getAclBindingsOrError());

    // When optimised, still need to add any topic level specific.
    aclBindingsOrErrors.addAll(buildBasicUsersAcls(project, false));
    return aclBindingsOrErrors;
  }

  private List<AclBindingsOrError> buildDetailedConsumerAndProducerAcls(Project project) {
    return buildBasicUsersAcls(project, true);
  }

  private List<AclBindingsOrError> buildBasicUsersAcls(
      Project project, boolean includeProjectLevel) {
    List<AclBindingsOrError> aclBindingsOrErrors = new ArrayList<>();
    project
        .getTopics()
        .forEach(
            topic -> {
              final String fullTopicName = topic.toString();
              Set<Consumer> consumers = new HashSet(topic.getConsumers());
              if (includeProjectLevel) {
                consumers.addAll(project.getConsumers());
              }
              if (!consumers.isEmpty()) {
                AclBindingsOrError aclBindingsOrError =
                    new ConsumerAclBindingsBuilder(
                            bindingsBuilder, new ArrayList<>(consumers), fullTopicName, false)
                        .getAclBindingsOrError();
                aclBindingsOrErrors.add(aclBindingsOrError);
              }
              Set<Producer> producers = new HashSet(topic.getProducers());
              if (includeProjectLevel) {
                producers.addAll(project.getProducers());
              }
              if (!producers.isEmpty()) {
                AclBindingsOrError aclBindingsOrError =
                    new ProducerAclBindingsBuilder(
                            bindingsBuilder, new ArrayList<>(producers), fullTopicName, false)
                        .getAclBindingsOrError();
                aclBindingsOrErrors.add(aclBindingsOrError);
              }
            });
    return aclBindingsOrErrors;
  }

  /**
   * Build a list of actions required to create or delete necessary bindings
   *
   * @param aclBindingsOrErrors List of pre computed actions based on a topology
   * @param bindings List of current bindings available in the cluster
   * @return List<Action> list of actions necessary to update the cluster
   */
  private List<Action> buildUpdateBindingsActions(
      List<AclBindingsOrError> aclBindingsOrErrors, Set<TopologyAclBinding> bindings)
      throws IOException {

    List<Action> updateActions = new ArrayList<>();

    List<Either> eitherStreamOrError =
        aclBindingsOrErrors.stream()
            .map(
                aclBindingsOrError -> {
                  return aclBindingsOrError.isError()
                      ? Either.Right(aclBindingsOrError.getErrorMessage())
                      : Either.Left(aclBindingsOrError.getAclBindings().stream());
                })
            .collect(Collectors.toList());

    List<String> errorMessages =
        eitherStreamOrError.stream()
            .filter(Either::isRight)
            .map(e -> (String) e.getRight().get())
            .collect(Collectors.toList());
    if (!errorMessages.isEmpty()) {
      for (String errorMessage : errorMessages) {
        LOGGER.error(errorMessage);
      }
      throw new IOException(errorMessages.get(0));
    }

    Set<TopologyAclBinding> allFinalBindings =
        eitherStreamOrError.stream()
            .filter(Either::isLeft)
            .flatMap(e -> (Stream<TopologyAclBinding>) e.getLeft().get())
            .collect(Collectors.toSet());

    Set<TopologyAclBinding> bindingsToBeCreated =
        allFinalBindings.stream()
            .filter(Objects::nonNull)
            // Only create what we manage
            .filter(this::matchesManagedPrefixList)
            // Diff of bindings, so we only create what is not already created in the cluster.
            .filter(binding -> !bindings.contains(binding))
            .collect(Collectors.toSet());

    if (!bindingsToBeCreated.isEmpty()) {
      CreateBindings createBindings = new CreateBindings(controlProvider, bindingsToBeCreated);
      updateActions.add(createBindings);
    }

    if (config.isAllowDeleteBindings()) {
      // clear acls that does not appear anymore in the new generated list,
      // but where previously created
      Set<TopologyAclBinding> bindingsToDelete =
          bindings.stream()
              .filter(binding -> !allFinalBindings.contains(binding))
              .collect(Collectors.toSet());
      if (!bindingsToDelete.isEmpty()) {
        ClearBindings clearBindings = new ClearBindings(controlProvider, bindingsToDelete);
        updateActions.add(clearBindings);
      }
    }
    return updateActions;
  }

  private boolean matchesManagedPrefixList(TopologyAclBinding topologyAclBinding) {
    String resourceName = topologyAclBinding.getResourceName();
    String principle = topologyAclBinding.getPrincipal();
    // For global wild cards ACL's we manage only if we manage the service account/principle,
    // regardless.
    if (resourceName.equals("*")) {
      return matchesServiceAccountPrefixList(principle);
    }

    if ("TOPIC".equalsIgnoreCase(topologyAclBinding.getResourceType())) {
      return matchesTopicPrefixList(resourceName);
    } else if ("GROUP".equalsIgnoreCase(topologyAclBinding.getResourceType())) {
      return matchesGroupPrefixList(resourceName);
    } else {
      return matchesServiceAccountPrefixList(principle);
    }
  }

  private boolean matchesTopicPrefixList(String topic) {
    return matchesPrefix(managedTopicPrefixes, topic, "Topic");
  }

  private boolean matchesGroupPrefixList(String group) {
    return matchesPrefix(managedGroupPrefixes, group, "Group");
  }

  private boolean matchesServiceAccountPrefixList(String principal) {
    return matchesPrefix(managedServiceAccountPrefixes, principal, "Principal");
  }

  private boolean matchesPrefix(List<String> prefixes, String item, String type) {
    boolean matches = prefixes.size() == 0 || prefixes.stream().anyMatch(item::startsWith);
    LOGGER.debug(String.format("%s %s matches %s with $s", type, item, matches, prefixes));
    return matches;
  }

  // Sync platform relevant Access Control List.
  private List<AclBindingsOrError> buildPlatformLevelActions(final Topology topology) {
    List<AclBindingsOrError> aclBindingsOrErrors = new ArrayList<>();
    Platform platform = topology.getPlatform();

    // Set cluster level ACLs
    syncClusterLevelRbac(platform.getKafka().getRbac(), KAFKA, aclBindingsOrErrors);
    syncClusterLevelRbac(platform.getKafkaConnect().getRbac(), KAFKA_CONNECT, aclBindingsOrErrors);
    syncClusterLevelRbac(
        platform.getSchemaRegistry().getRbac(), SCHEMA_REGISTRY, aclBindingsOrErrors);

    // Set component level ACLs
    for (SchemaRegistryInstance schemaRegistry : platform.getSchemaRegistry().getInstances()) {
      aclBindingsOrErrors.add(
          new SchemaRegistryAclBindingsBuilder(bindingsBuilder, schemaRegistry)
              .getAclBindingsOrError());
    }
    for (ControlCenterInstance controlCenter : platform.getControlCenter().getInstances()) {
      aclBindingsOrErrors.add(
          new ControlCenterAclBindingsBuilder(bindingsBuilder, controlCenter)
              .getAclBindingsOrError());
    }

    for (KsqlServerInstance ksqlServer : platform.getKsqlServer().getInstances()) {
      aclBindingsOrErrors.add(
          new KSqlServerAclBindingsBuilder(bindingsBuilder, ksqlServer).getAclBindingsOrError());
    }

    return aclBindingsOrErrors;
  }

  private void syncClusterLevelRbac(
      Optional<Map<String, List<User>>> rbac,
      Component cmp,
      List<AclBindingsOrError> aclBindingsOrErrors) {
    if (rbac.isPresent()) {
      Map<String, List<User>> roles = rbac.get();
      for (String role : roles.keySet()) {
        for (User user : roles.get(role)) {
          aclBindingsOrErrors.add(
              new ClusterLevelAclBindingsBuilder(bindingsBuilder, role, user, cmp)
                  .getAclBindingsOrError());
        }
      }
    }
  }

  private void syncRbacRawRoles(
      Map<String, List<String>> rbacRawRoles,
      String topicPrefix,
      List<AclBindingsOrError> aclBindingsOrErrors) {
    rbacRawRoles.forEach(
        (predefinedRole, principals) ->
            principals.forEach(
                principal ->
                    aclBindingsOrErrors.add(
                        new PredefinedAclBindingsBuilder(
                                bindingsBuilder, principal, predefinedRole, topicPrefix)
                            .getAclBindingsOrError())));
  }

  private Optional<AclBindingsOrError> syncApplicationAcls(DynamicUser app, String topicPrefix) {
    AclBindingsOrError aclBindingsOrError = null;
    if (app instanceof KStream) {
      aclBindingsOrError =
          new KStreamsAclBindingsBuilder(bindingsBuilder, (KStream) app, topicPrefix)
              .getAclBindingsOrError();
    } else if (app instanceof Connector) {
      aclBindingsOrError =
          new KConnectAclBindingsBuilder(bindingsBuilder, (Connector) app, topicPrefix)
              .getAclBindingsOrError();
    } else if (app instanceof KSqlApp) {
      aclBindingsOrError =
          new KSqlAppAclBindingsBuilder(bindingsBuilder, (KSqlApp) app, topicPrefix)
              .getAclBindingsOrError();
    }
    return Optional.ofNullable(aclBindingsOrError);
  }

  @Override
  public void printCurrentState(PrintStream out) {
    out.println("List of ACLs: ");
    controlProvider
        .listAcls()
        .forEach(
            (topic, aclBindings) -> {
              out.println(topic);
              aclBindings.forEach(out::println);
            });
  }
}
