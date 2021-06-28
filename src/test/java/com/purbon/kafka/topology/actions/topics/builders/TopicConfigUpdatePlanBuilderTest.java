package com.purbon.kafka.topology.actions.topics.builders;

import com.purbon.kafka.topology.TopicManager;
import com.purbon.kafka.topology.actions.topics.TopicConfigUpdatePlan;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.model.Impl.TopicImpl;
import com.purbon.kafka.topology.model.Topic;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

public class TopicConfigUpdatePlanBuilderTest {

  private static final String TOPIC_NAME = "foo";
  public static final String DEFAULT_RETENTION_MS = "604800000";
  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private TopologyBuilderAdminClient adminClient;

  @Test
  public void shouldNotChangeConfigWhenNoConfig() {
    doReturn(createEmptyConfig()).when(adminClient).getActualTopicConfig(TOPIC_NAME);
    Topic topic = createTopic();
    TopicConfigUpdatePlan plan = getTopicConfigUpdatePlan(topic);
    assertNewUpdatedAndDeletedCounts(plan, 0, 0, 0);
  }

  @Test
  public void shouldNotAddNewConfigForNumPartitionsButShouldUpdateFlag() {
    doReturn(createEmptyConfig()).when(adminClient).getActualTopicConfig(TOPIC_NAME);
    Topic topic = createTopic(TopicManager.NUM_PARTITIONS, "5");
    TopicConfigUpdatePlan plan = getTopicConfigUpdatePlan(topic);
    assertNewUpdatedAndDeletedCounts(plan, 0, 0, 0);
    assertTrue(plan.isUpdatePartitionCount());
  }

  @Test
  public void shouldAddNewConfigForRetention() {
    doReturn(createDefaultRetentionConfig()).when(adminClient).getActualTopicConfig(TOPIC_NAME);
    Topic topic = createTopic(TopicConfig.RETENTION_MS_CONFIG, "1000");
    TopicConfigUpdatePlan plan = getTopicConfigUpdatePlan(topic);
    assertNewUpdatedAndDeletedCounts(plan, 1, 0, 0);
  }

  @Test
  public void shouldUpdateConfigForRetention() {
    doReturn(createAlreadyOverriddenRetentionConfig()).when(adminClient).getActualTopicConfig(TOPIC_NAME);
    Topic topic = createTopic(TopicConfig.RETENTION_MS_CONFIG, "1000");
    TopicConfigUpdatePlan plan = getTopicConfigUpdatePlan(topic);
    assertNewUpdatedAndDeletedCounts(plan, 0, 1, 0);
  }

  @Test
  public void shouldDeleteConfigForRetention() {
    doReturn(createAlreadyOverriddenRetentionConfig()).when(adminClient).getActualTopicConfig(TOPIC_NAME);
    Topic topic = createTopic();
    TopicConfigUpdatePlan plan = getTopicConfigUpdatePlan(topic);
    assertNewUpdatedAndDeletedCounts(plan, 0, 0, 1);
  }

  private TopicConfigUpdatePlan getTopicConfigUpdatePlan(Topic topic) {
    TopicConfigUpdatePlanBuilder builder = new TopicConfigUpdatePlanBuilder(adminClient);
    return builder.createTopicConfigUpdatePlan(topic, TOPIC_NAME);
  }

  private Topic createTopic(String configName, String configValue) {
    var config = new HashMap<String, String>();
    config.put(configName, configValue);
    return new TopicImpl(TopicConfigUpdatePlanBuilderTest.TOPIC_NAME, config);
  }

  private Topic createTopic() {
    return new TopicImpl(TopicConfigUpdatePlanBuilderTest.TOPIC_NAME);
  }

  private Config createEmptyConfig() {
    return new Config(Collections.emptyList());
  }

  private Config createDefaultRetentionConfig() {
    ConfigEntry configEntry = createRetentionConfig(DEFAULT_RETENTION_MS, ConfigEntry.ConfigSource.DEFAULT_CONFIG);
    return new Config(Collections.singletonList(configEntry));
  }

  private Config createAlreadyOverriddenRetentionConfig() {
    ConfigEntry configEntry = createRetentionConfig("432000000", ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
    return new Config(Collections.singletonList(configEntry));
  }

  private ConfigEntry createRetentionConfig(final String s, final ConfigEntry.ConfigSource dynamicTopicConfig) {
    return new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, s, dynamicTopicConfig, false, false, Collections.emptyList(), ConfigEntry.ConfigType.LONG, null);
  }

  private void assertNewUpdatedAndDeletedCounts(TopicConfigUpdatePlan plan, int expectedNew, int expectedUpdated, int expectedDeleted) {
    assertEquals(expectedNew, plan.getNewConfigValues().size());
    assertEquals(expectedUpdated, plan.getUpdatedConfigValues().size());
    assertEquals(expectedDeleted, plan.getDeletedConfigValues().size());
  }
}
