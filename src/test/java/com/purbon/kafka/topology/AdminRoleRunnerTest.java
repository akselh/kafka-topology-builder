package com.purbon.kafka.topology;

import static com.purbon.kafka.topology.api.mds.ClusterIDs.CONNECT_CLUSTER_ID_LABEL;
import static com.purbon.kafka.topology.api.mds.ClusterIDs.SCHEMA_REGISTRY_CLUSTER_ID_LABEL;
import static com.purbon.kafka.topology.api.mds.RequestScope.RESOURCE_NAME;
import static com.purbon.kafka.topology.roles.RBACPredefinedRoles.SECURITY_ADMIN;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.purbon.kafka.topology.api.mds.ClusterIDs;
import com.purbon.kafka.topology.api.mds.MDSApiClient;
import com.purbon.kafka.topology.exceptions.ConfigurationException;
import com.purbon.kafka.topology.model.users.Connector;
import com.purbon.kafka.topology.roles.AdminRoleRunner;
import java.io.IOException;
import java.util.Map;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class AdminRoleRunnerTest {

  @Mock MDSApiClient apiClient;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  private Connector connector;

  private static ClusterIDs allClusterIDs;
  private static ClusterIDs nonClusterIDs;

  @BeforeClass
  public static void beforeClass() {

    allClusterIDs = new ClusterIDs();
    nonClusterIDs = new ClusterIDs();

    allClusterIDs.setConnectClusterID("1234");
    allClusterIDs.setSchemaRegistryClusterID("4321");
    allClusterIDs.setKafkaClusterId("abcd");
  }

  @Before
  public void before() {
    connector = new Connector();
  }

  @Test
  public void testWithAllClientIdsForConnect() throws IOException {
    when(apiClient.withClusterIDs()).thenReturn(allClusterIDs);

    AdminRoleRunner runner = new AdminRoleRunner("foo", SECURITY_ADMIN, apiClient);
    runner.forKafkaConnect(connector);

    assertEquals("kafka-connect", runner.getScope().getResource(0).get(RESOURCE_NAME));

    String connectClusterId = "1234";
    Map<String, String> clusterIDs = runner.getScope().getClusterIDs();
    assertEquals(connectClusterId, clusterIDs.get(CONNECT_CLUSTER_ID_LABEL));
  }

  @Test(expected = ConfigurationException.class)
  public void testWithMissingClientIdsForConnect() throws IOException {
    when(apiClient.withClusterIDs()).thenReturn(nonClusterIDs);

    AdminRoleRunner runner = new AdminRoleRunner("foo", SECURITY_ADMIN, apiClient);
    runner.forKafkaConnect(connector);
  }

  @Test
  public void testWithAllClientIdsForSchemaRegistry() throws IOException {
    when(apiClient.withClusterIDs()).thenReturn(allClusterIDs);

    AdminRoleRunner runner = new AdminRoleRunner("foo", SECURITY_ADMIN, apiClient);
    runner.forSchemaRegistry();

    String connectClusterId = "4321";
    Map<String, String> clusterIDs = runner.getScope().getClusterIDs();
    assertEquals(connectClusterId, clusterIDs.get(SCHEMA_REGISTRY_CLUSTER_ID_LABEL));
  }

  @Test(expected = ConfigurationException.class)
  public void testWithMissingClientIdsForSchemaRegistry() throws IOException {
    when(apiClient.withClusterIDs()).thenReturn(nonClusterIDs);

    AdminRoleRunner runner = new AdminRoleRunner("foo", SECURITY_ADMIN, apiClient);
    runner.forSchemaRegistry();
  }
}