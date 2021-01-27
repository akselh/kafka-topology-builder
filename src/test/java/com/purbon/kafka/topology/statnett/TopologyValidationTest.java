package com.purbon.kafka.topology.statnett;

import static com.purbon.kafka.topology.BuilderCLI.ADMIN_CLIENT_CONFIG_OPTION;
import static com.purbon.kafka.topology.BuilderCLI.BROKERS_OPTION;
import static org.assertj.core.api.Assertions.assertThat;

import com.purbon.kafka.topology.TopologyBuilderConfig;
import com.purbon.kafka.topology.TopologyValidator;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.serdes.TopologySerdes;
import com.purbon.kafka.topology.utils.TestUtils;
import java.util.*;
import org.junit.Before;
import org.junit.Test;

public class TopologyValidationTest {

  static final String TOPOLOGY_VALIDATIONS_CONFIG = "topology.validations";

  private TopologySerdes parser;

  @Before
  public void setup() {
    parser = new TopologySerdes();
  }

  private TopologyValidator setupTopologyValidator(String... validationConfig) {
    Map<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");
    cliOps.put(ADMIN_CLIENT_CONFIG_OPTION, "/fooBar");
    Properties props = new Properties();
    props.put(TOPOLOGY_VALIDATIONS_CONFIG, Arrays.asList(validationConfig));
    TopologyBuilderConfig config = new TopologyBuilderConfig(cliOps, props);
    return new TopologyValidator(config);
  }

  @Test
  public void shouldHandleValidTopology() {
    TopologyValidator validator =
        setupTopologyValidator(
            "statnett.ConnectorGroupValidation",
            "statnett.DuplicateTopicValidation",
            "statnett.StreamApplicationIdValidation",
            "statnett.TopicConfigValidation",
            "statnett.TopicNameValidation");
    Topology topology = parser.deserialise(TestUtils.getResourceFile("/statnett/descriptor.yaml"));

    List<String> results = validator.validate(topology);
    assertThat(results).isEmpty();
  }

  @Test
  public void shouldFailOnMissingApplicationId() {
    TopologyValidator validator =
        setupTopologyValidator(
            "com.purbon.kafka.topology.validation.statnett.StreamApplicationIdValidation");
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/statnett/descriptor-no-applicationId.yaml"));

    List<String> results = validator.validate(topology);
    assertThat(results).hasSize(1);
  }

  @Test
  public void shouldFailOnMissingConnectorGroup() {
    TopologyValidator validator = setupTopologyValidator("statnett.ConnectorGroupValidation");
    Topology topology =
        parser.deserialise(
            TestUtils.getResourceFile("/statnett/descriptor-no-group-for-connector.yaml"));

    List<String> results = validator.validate(topology);
    assertThat(results).hasSize(1);
  }

  @Test
  public void shouldFailOnDuplicateTopic() {
    TopologyValidator validator = setupTopologyValidator("statnett.DuplicateTopicValidation");
    Topology topology =
        parser.deserialise(TestUtils.getResourceFile("/statnett/descriptor-duplicate-topic.yaml"));

    List<String> results = validator.validate(topology);
    assertThat(results).hasSize(1);
  }

  @Test
  public void shouldFailOnIllegalTopicName() {
    TopologyValidator validator = setupTopologyValidator("statnett.TopicNameValidation");
    Topology topology =
        parser.deserialise(
            TestUtils.getResourceFile("/statnett/descriptor-illegal-topic-name.yaml"));

    List<String> results = validator.validate(topology);
    assertThat(results).hasSize(1);
  }

  @Test
  public void shouldFailOnIllegalTopicConfig() {
    TopologyValidator validator = setupTopologyValidator("statnett.TopicConfigValidation");
    Topology topology =
        parser.deserialise(
            TestUtils.getResourceFile("/statnett/descriptor-illegal-topic-config.yaml"));

    List<String> results = validator.validate(topology);
    assertThat(results).hasSize(1);
  }
}
