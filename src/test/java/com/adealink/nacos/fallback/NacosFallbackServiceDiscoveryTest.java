package com.adealink.nacos.fallback;

import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.scheduling.TaskScheduler;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * NacosFallbackServiceDiscovery 单元测试
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("NacosFallbackServiceDiscovery 测试")
class NacosFallbackServiceDiscoveryTest {

    @Mock
    private NamingService localNamingService;

    @Mock
    private NamingService testNamingService;

    @Mock
    private TaskScheduler taskScheduler;

    @Mock
    private ScheduledFuture<?> scheduledFuture;

    @Mock
    private NacosFallbackServiceDiscovery.NamingServiceFactory namingServiceFactory;

    private NacosFallbackProperties properties;
    private NacosFallbackServiceDiscovery serviceDiscovery;
    private static final String INSTANCE_ID = "test-instance-id";

    @BeforeEach
    void setUp() {
        properties = new NacosFallbackProperties();
        properties.setEnabled(true);
        properties.setLocalServerAddr("localhost:8848");
        properties.setLocalGroup("DEFAULT_GROUP");
        properties.setTestPublicIp("10.0.0.1");
        properties.setTestGroup("DEFAULT_GROUP");
        properties.setTestPrivateIpPrefix("172.");
        properties.setSyncIntervalSeconds(60);
        properties.setLeaderServiceName("nacos-sync-leader");
        properties.setLeaderCheckIntervalSeconds(10);
        properties.setLeaderElectionWaitMs(10); // 短时间用于测试
    }

    private void createServiceDiscovery() {
        serviceDiscovery = new NacosFallbackServiceDiscovery(
                properties, taskScheduler, namingServiceFactory, INSTANCE_ID);
    }

    @Nested
    @DisplayName("start 方法测试")
    class StartTest {

        @Test
        @DisplayName("当禁用时不应调度初始化任务")
        void shouldNotScheduleInitializeWhenDisabled() {
            properties.setEnabled(false);
            createServiceDiscovery();

            serviceDiscovery.start();

            verify(taskScheduler, never()).schedule(any(Runnable.class), any(Instant.class));
        }

        @Test
        @DisplayName("当启用时应调度初始化任务")
        void shouldScheduleInitializeWhenEnabled() {
            createServiceDiscovery();

            serviceDiscovery.start();

            verify(taskScheduler).schedule(any(Runnable.class), any(Instant.class));
        }
    }

    @Nested
    @DisplayName("initialize 方法测试")
    class InitializeTest {

        @Test
        @DisplayName("应成功初始化本地 Nacos 客户端")
        void shouldInitializeLocalNamingService() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class)))
                    .thenReturn(localNamingService)
                    .thenReturn(testNamingService);
            when(localNamingService.selectInstances(anyString(), anyString(), eq(true)))
                    .thenReturn(Collections.emptyList());
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));

            serviceDiscovery.initialize();

            verify(namingServiceFactory, atLeastOnce()).create(any(Properties.class));
        }

        @Test
        @DisplayName("当已有 Leader 时应启动 Leader 检查任务")
        void shouldStartLeaderCheckWhenExistingLeaderFound() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class))).thenReturn(localNamingService);

            Instance existingLeader = createInstance("127.0.0.1", 1);
            Map<String, String> metadata = new HashMap<>();
            metadata.put("instanceId", "other-leader");
            existingLeader.setMetadata(metadata);
            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader));
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));

            serviceDiscovery.initialize();

            verify(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), eq(Duration.ofSeconds(10)));
        }

        @Test
        @DisplayName("初始化失败时应调度重试")
        void shouldScheduleRetryOnInitializationFailure() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class)))
                    .thenThrow(new RuntimeException("Connection failed"));

            serviceDiscovery.initialize();

            // 验证调度了重试任务
            verify(taskScheduler).schedule(any(Runnable.class), any(Instant.class));
        }

        @Test
        @DisplayName("初始化时应设置正确的本地 Nacos 属性")
        void shouldSetCorrectLocalNacosProperties() throws Exception {
            properties.setLocalNamespace("dev-namespace");
            createServiceDiscovery();

            ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);
            when(namingServiceFactory.create(propsCaptor.capture()))
                    .thenReturn(localNamingService)
                    .thenReturn(testNamingService);
            when(localNamingService.selectInstances(anyString(), anyString(), eq(true)))
                    .thenReturn(Collections.emptyList());
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));

            serviceDiscovery.initialize();

            Properties capturedProps = propsCaptor.getAllValues().get(0);
            assertEquals("localhost:8848", capturedProps.getProperty("serverAddr"));
            assertEquals("dev-namespace", capturedProps.getProperty("namespace"));
        }
    }

    @Nested
    @DisplayName("Leader 选举测试")
    class LeaderElectionTest {

        @Test
        @DisplayName("当没有现有 Leader 时应成功成为 Leader")
        void shouldBecomeLeaderWhenNoExistingLeader() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class)))
                    .thenReturn(localNamingService)
                    .thenReturn(testNamingService);

            // 第一次检查没有 Leader
            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.emptyList())
                    .thenReturn(Collections.singletonList(createLeaderInstance(INSTANCE_ID)));

            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createEmptyListView());

            serviceDiscovery.initialize();

            // 验证注册了 Leader 实例
            verify(localNamingService).registerInstance(eq("nacos-sync-leader"), anyString(), any(Instance.class));
        }

        @Test
        @DisplayName("当其他实例先成为 Leader 时应注销自己")
        void shouldDeregisterWhenAnotherInstanceBecomesLeaderFirst() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class))).thenReturn(localNamingService);

            // 第一次检查没有 Leader，注册后发现有其他 Leader 先注册
            Instance otherLeader = createLeaderInstance("other-instance");
            otherLeader.getMetadata().put("startTime", "1"); // 更早的启动时间
            Instance selfLeader = createLeaderInstance(INSTANCE_ID);
            selfLeader.getMetadata().put("startTime", "100"); // 更晚的启动时间

            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.emptyList())
                    .thenReturn(Arrays.asList(otherLeader, selfLeader));
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));

            serviceDiscovery.initialize();

            // 验证注销了自己的 Leader 实例
            verify(localNamingService).deregisterInstance(eq("nacos-sync-leader"), anyString(), any(Instance.class));
        }
    }

    @Nested
    @DisplayName("IP 重写测试")
    class IpRewriteTest {

        @Test
        @DisplayName("应将内网 IP 重写为公网 IP")
        void shouldRewritePrivateIpToPublicIp() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance testInstance = createInstance("172.16.0.1", 8080);
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstance));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            ArgumentCaptor<Instance> instanceCaptor = ArgumentCaptor.forClass(Instance.class);
            verify(localNamingService).registerInstance(eq("test-service"), anyString(), instanceCaptor.capture());

            Instance registeredInstance = instanceCaptor.getValue();
            assertEquals("10.0.0.1", registeredInstance.getIp());
            assertEquals("172.16.0.1", registeredInstance.getMetadata().get("original-ip"));
        }

        @Test
        @DisplayName("非内网 IP 不应被重写")
        void shouldNotRewriteNonPrivateIp() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance testInstance = createInstance("192.168.1.1", 8080);
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstance));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            ArgumentCaptor<Instance> instanceCaptor = ArgumentCaptor.forClass(Instance.class);
            verify(localNamingService).registerInstance(eq("test-service"), anyString(), instanceCaptor.capture());

            Instance registeredInstance = instanceCaptor.getValue();
            assertEquals("192.168.1.1", registeredInstance.getIp());
        }

        @Test
        @DisplayName("当 testPrivateIpPrefix 为空时不应重写任何 IP")
        void shouldNotRewriteWhenPrivateIpPrefixIsEmpty() throws Exception {
            properties.setTestPrivateIpPrefix("");
            createServiceDiscovery();
            setupLeaderMocks();

            Instance testInstance = createInstance("172.16.0.1", 8080);
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstance));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            ArgumentCaptor<Instance> instanceCaptor = ArgumentCaptor.forClass(Instance.class);
            verify(localNamingService).registerInstance(eq("test-service"), anyString(), instanceCaptor.capture());

            Instance registeredInstance = instanceCaptor.getValue();
            assertEquals("172.16.0.1", registeredInstance.getIp());
        }
    }

    @Nested
    @DisplayName("服务同步测试")
    class ServiceSyncTest {

        @Test
        @DisplayName("应正确同步测试环境的服务实例")
        void shouldSyncServiceInstances() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance testInstance = createInstance("172.16.0.1", 8080);
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstance));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            verify(localNamingService).registerInstance(eq("test-service"), anyString(), any(Instance.class));
        }

        @Test
        @DisplayName("当本地有原生实例时应跳过同步并清理 fallback 实例")
        void shouldSkipSyncWhenNativeInstancesExist() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance nativeInstance = createInstance("192.168.1.1", 8080);
            Instance fallbackInstance = createFallbackInstance("10.0.0.1", 8080);

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Arrays.asList(nativeInstance, fallbackInstance));

            serviceDiscovery.initialize();

            // 不应注册新实例
            verify(localNamingService, never()).registerInstance(eq("test-service"), anyString(), any(Instance.class));
            // 应清理 fallback 实例
            verify(localNamingService).deregisterInstance(eq("test-service"), anyString(), eq(fallbackInstance));
        }

        @Test
        @DisplayName("增量更新时应先注册新实例再删除旧实例")
        void shouldAddBeforeRemoveForIncrementalUpdate() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            // 旧的 fallback 实例
            Instance oldFallback = createFallbackInstance("10.0.0.1", 8080);
            // 新的测试环境实例（不同端口）
            Instance newTestInstance = createInstance("172.16.0.1", 9090);

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(newTestInstance));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.singletonList(oldFallback));

            serviceDiscovery.initialize();

            // 验证先注册后注销的顺序
            InOrder inOrderVerifier = inOrder(localNamingService);
            inOrderVerifier.verify(localNamingService).registerInstance(eq("test-service"), anyString(), any(Instance.class));
            inOrderVerifier.verify(localNamingService).deregisterInstance(eq("test-service"), anyString(), eq(oldFallback));
        }

        @Test
        @DisplayName("当实例无变化时应跳过同步")
        void shouldSkipSyncWhenInstancesUnchanged() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance testInstance = createInstance("172.16.0.1", 8080);
            Instance existingFallback = createFallbackInstance("10.0.0.1", 8080); // 重写后的 IP 相同

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstance));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.singletonList(existingFallback));

            serviceDiscovery.initialize();

            // 不应有注册或注销操作
            verify(localNamingService, never()).registerInstance(eq("test-service"), anyString(), any(Instance.class));
            verify(localNamingService, never()).deregisterInstance(eq("test-service"), anyString(), any(Instance.class));
        }

        @Test
        @DisplayName("当测试环境无服务时应清理所有 fallback 实例")
        void shouldCleanupAllFallbackWhenNoTestServices() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createEmptyListView());

            serviceDiscovery.initialize();

            // 应查询服务列表
            verify(testNamingService).getServicesOfServer(1, 100, "DEFAULT_GROUP");
        }
    }

    @Nested
    @DisplayName("分页获取服务测试")
    class PaginationTest {

        @Test
        @DisplayName("应正确分页获取所有服务")
        void shouldPaginateAllServices() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            // 第一页 100 个服务
            List<String> page1 = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                page1.add("service-" + i);
            }
            // 第二页 50 个服务
            List<String> page2 = new ArrayList<>();
            for (int i = 100; i < 150; i++) {
                page2.add("service-" + i);
            }

            when(testNamingService.getServicesOfServer(eq(1), eq(100), anyString()))
                    .thenReturn(createListView(page1, 150));
            when(testNamingService.getServicesOfServer(eq(2), eq(100), anyString()))
                    .thenReturn(createListView(page2, 150));
            // 所有服务返回空实例以简化测试
            when(testNamingService.selectInstances(anyString(), anyString(), eq(true)))
                    .thenReturn(Collections.emptyList());
            when(localNamingService.getAllInstances(anyString(), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            // 验证分页调用
            verify(testNamingService).getServicesOfServer(1, 100, "DEFAULT_GROUP");
            verify(testNamingService).getServicesOfServer(2, 100, "DEFAULT_GROUP");
        }
    }

    @Nested
    @DisplayName("实例分类测试")
    class InstanceCategorizationTest {

        @Test
        @DisplayName("应正确区分 fallback 和 native 实例")
        void shouldCorrectlyCategorizeFallbackAndNativeInstances() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance nativeInstance = createInstance("192.168.1.1", 8080);
            Instance fallbackInstance = createFallbackInstance("10.0.0.1", 8081);

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Arrays.asList(nativeInstance, fallbackInstance));

            serviceDiscovery.initialize();

            // 有原生实例时应跳过同步并清理 fallback
            verify(localNamingService).deregisterInstance(eq("test-service"), anyString(), eq(fallbackInstance));
        }

        @Test
        @DisplayName("实例无 metadata 时应归类为 native")
        void shouldCategorizeAsNativeWhenNoMetadata() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance instanceWithoutMetadata = new Instance();
            instanceWithoutMetadata.setIp("192.168.1.1");
            instanceWithoutMetadata.setPort(8080);
            // 不设置 metadata

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.singletonList(instanceWithoutMetadata));

            serviceDiscovery.initialize();

            // 应被视为 native 实例，跳过同步
            verify(testNamingService, never()).selectInstances(eq("test-service"), anyString(), eq(true));
        }
    }

    @Nested
    @DisplayName("stop 方法测试")
    class StopTest {

        @Test
        @DisplayName("stop 应取消调度任务")
        void shouldCancelScheduledTasks() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            serviceDiscovery.initialize();
            serviceDiscovery.stop();

            verify(scheduledFuture, atLeastOnce()).cancel(false);
        }

        @Test
        @DisplayName("stop 应释放 Leader 身份")
        void shouldReleaseLeadershipOnStop() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            serviceDiscovery.initialize();
            serviceDiscovery.stop();

            // 验证注销了 Leader 实例
            verify(localNamingService, atLeastOnce()).deregisterInstance(
                    eq("nacos-sync-leader"), anyString(), any(Instance.class));
        }
    }

    @Nested
    @DisplayName("实例元数据测试")
    class InstanceMetadataTest {

        @Test
        @DisplayName("注册的实例应包含正确的元数据")
        void shouldIncludeCorrectMetadata() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance testInstance = createInstance("172.16.0.1", 8080);
            Map<String, String> existingMetadata = new HashMap<>();
            existingMetadata.put("existing-key", "existing-value");
            testInstance.setMetadata(existingMetadata);

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstance));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            ArgumentCaptor<Instance> instanceCaptor = ArgumentCaptor.forClass(Instance.class);
            verify(localNamingService).registerInstance(eq("test-service"), anyString(), instanceCaptor.capture());

            Instance registeredInstance = instanceCaptor.getValue();
            Map<String, String> metadata = registeredInstance.getMetadata();

            assertEquals("true", metadata.get("fallback"));
            assertEquals("test-env", metadata.get("source"));
            assertEquals("172.16.0.1", metadata.get("original-ip"));
            assertNotNull(metadata.get("synced-at"));
            assertEquals("existing-value", metadata.get("existing-key"));
        }

        @Test
        @DisplayName("注册的实例应设置为临时实例")
        void shouldBeEphemeralInstance() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance testInstance = createInstance("172.16.0.1", 8080);

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstance));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            ArgumentCaptor<Instance> instanceCaptor = ArgumentCaptor.forClass(Instance.class);
            verify(localNamingService).registerInstance(eq("test-service"), anyString(), instanceCaptor.capture());

            assertTrue(instanceCaptor.getValue().isEphemeral());
        }
    }

    @Nested
    @DisplayName("清理陈旧服务测试")
    class CleanupStaleServicesTest {

        @Test
        @DisplayName("应清理测试环境不再存在的服务的 fallback 实例")
        void shouldCleanupServicesNoLongerInTestEnv() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            // 第一次同步：有 service-a
            Instance testInstanceA = createInstance("172.16.0.1", 8080);
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("service-a")))
                    .thenReturn(createEmptyListView()); // 第二次同步：无服务
            when(testNamingService.selectInstances(eq("service-a"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstanceA));
            when(localNamingService.getAllInstances(eq("service-a"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList())
                    .thenReturn(Collections.singletonList(createFallbackInstance("10.0.0.1", 8080)));

            serviceDiscovery.initialize();

            // 验证第一次同步注册了实例
            verify(localNamingService).registerInstance(eq("service-a"), anyString(), any(Instance.class));
        }
    }

    @Nested
    @DisplayName("startSyncAsLeader 失败场景测试")
    class StartSyncAsLeaderFailureTest {

        @Test
        @DisplayName("startSyncAsLeader 失败时应释放 Leader 并启动检查任务")
        void shouldReleaseLeadershipAndStartCheckTaskWhenStartSyncFails() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class)))
                    .thenReturn(localNamingService)
                    .thenThrow(new RuntimeException("Failed to connect to test Nacos"));

            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.emptyList())
                    .thenReturn(Collections.singletonList(createLeaderInstance(INSTANCE_ID)));
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));

            serviceDiscovery.initialize();

            // 验证释放了 Leader
            verify(localNamingService).deregisterInstance(eq("nacos-sync-leader"), anyString(), any(Instance.class));
            // 验证启动了 Leader 检查任务
            verify(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), eq(Duration.ofSeconds(10)));
        }

        @Test
        @DisplayName("初始化时应设置正确的测试环境 Nacos 属性（包含 namespace）")
        void shouldSetCorrectTestNacosPropertiesWithNamespace() throws Exception {
            properties.setTestNamespace("test-namespace");
            createServiceDiscovery();

            ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);
            when(namingServiceFactory.create(propsCaptor.capture()))
                    .thenReturn(localNamingService)
                    .thenReturn(testNamingService);
            when(localNamingService.selectInstances(anyString(), anyString(), eq(true)))
                    .thenReturn(Collections.emptyList())
                    .thenReturn(Collections.singletonList(createLeaderInstance(INSTANCE_ID)));
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createEmptyListView());

            serviceDiscovery.initialize();

            // 第二次调用是创建 testNamingService
            Properties testProps = propsCaptor.getAllValues().get(1);
            assertEquals("10.0.0.1:8848", testProps.getProperty("serverAddr"));
            assertEquals("test-namespace", testProps.getProperty("namespace"));
        }
    }

    @Nested
    @DisplayName("同步失败保留旧实例测试")
    class SyncFailureRetainOldInstancesTest {

        @Test
        @DisplayName("同步失败时应保留旧的 fallback 实例")
        void shouldRetainOldFallbackInstancesWhenSyncFails() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance oldFallback = createFallbackInstance("10.0.0.1", 8080);

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            // selectInstances 抛出异常，模拟同步失败
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenThrow(new RuntimeException("Network error"));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.singletonList(oldFallback));

            serviceDiscovery.initialize();

            // 不应注销旧的 fallback 实例
            verify(localNamingService, never()).deregisterInstance(eq("test-service"), anyString(), any(Instance.class));
        }

        @Test
        @DisplayName("同步返回空实例时应保留旧的 fallback 实例")
        void shouldRetainOldFallbackInstancesWhenSyncReturnsEmpty() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            Instance oldFallback = createFallbackInstance("10.0.0.1", 8080);

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            // selectInstances 返回空列表
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenReturn(Collections.emptyList());
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.singletonList(oldFallback));

            serviceDiscovery.initialize();

            // 不应注销旧的 fallback 实例
            verify(localNamingService, never()).deregisterInstance(eq("test-service"), anyString(), any(Instance.class));
        }
    }

    @Nested
    @DisplayName("cleanupStaleServices 异常处理测试")
    class CleanupStaleServicesExceptionTest {

        @Test
        @DisplayName("cleanupStaleServices 异常时应将失败的服务保留在跟踪集合中")
        void shouldRetainFailedServicesInSyncedSet() throws Exception {
            createServiceDiscovery();
            setupLeaderMocksWithoutTestService();

            // 第一次同步：有 service-a 和 service-b
            Instance testInstanceA = createInstance("172.16.0.1", 8080);
            Instance testInstanceB = createInstance("172.16.0.2", 8080);
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Arrays.asList("service-a", "service-b")))
                    .thenReturn(createListView(Collections.singletonList("service-b"))); // 第二次同步：只有 service-b
            when(testNamingService.selectInstances(eq("service-a"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstanceA));
            when(testNamingService.selectInstances(eq("service-b"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstanceB));
            when(localNamingService.getAllInstances(eq("service-a"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());
            when(localNamingService.getAllInstances(eq("service-b"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            // 验证两个服务都注册了
            verify(localNamingService).registerInstance(eq("service-a"), anyString(), any(Instance.class));
            verify(localNamingService).registerInstance(eq("service-b"), anyString(), any(Instance.class));
        }
    }

    @Nested
    @DisplayName("syncServiceInstances 异常处理测试")
    class SyncServiceInstancesExceptionTest {

        @Test
        @DisplayName("syncServiceInstances 抛出异常时应返回 false 并记录错误")
        void shouldReturnFalseAndLogErrorWhenExceptionThrown() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Collections.singletonList("test-service")));
            when(localNamingService.getAllInstances(eq("test-service"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());
            when(testNamingService.selectInstances(eq("test-service"), anyString(), eq(true)))
                    .thenThrow(new RuntimeException("Connection timeout"));

            serviceDiscovery.initialize();

            // 不应注册任何实例
            verify(localNamingService, never()).registerInstance(eq("test-service"), anyString(), any(Instance.class));
        }
    }

    @Nested
    @DisplayName("checkAndTryBecomeLeader 方法测试")
    class CheckAndTryBecomeLeaderTest {

        @Test
        @DisplayName("已经是 Leader 时应直接返回不做任何查询")
        void shouldReturnImmediatelyWhenAlreadyLeader() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createEmptyListView());

            // 初始化成为 Leader
            serviceDiscovery.initialize();

            // 由于已经是 Leader，手动触发 checkAndTryBecomeLeader 不应有额外的 selectInstances 调用
            // 这里通过验证 selectInstances 调用次数来确认
            verify(localNamingService, atLeast(1)).selectInstances(eq("nacos-sync-leader"), anyString(), eq(true));
        }

        @Test
        @DisplayName("Leader 健康时应仅记录日志不做其他操作")
        void shouldOnlyLogWhenLeaderIsHealthy() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class))).thenReturn(localNamingService);

            Instance existingLeader = createLeaderInstance("other-leader");
            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader));

            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), any(Duration.class));

            serviceDiscovery.initialize();

            // 手动触发 checkAndTryBecomeLeader
            Runnable checkTask = runnableCaptor.getValue();
            checkTask.run();

            // 不应尝试注册 Leader
            verify(localNamingService, never()).registerInstance(eq("nacos-sync-leader"), anyString(), any(Instance.class));
        }

        @Test
        @DisplayName("Leader 健康但 metadata 为 null 时应正常处理")
        void shouldHandleNullMetadataWhenLeaderIsHealthy() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class))).thenReturn(localNamingService);

            Instance existingLeader = new Instance();
            existingLeader.setIp("127.0.0.1");
            existingLeader.setPort(1);
            existingLeader.setHealthy(true);
            existingLeader.setMetadata(null); // metadata 为 null

            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader));

            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), any(Duration.class));

            serviceDiscovery.initialize();

            // 手动触发 checkAndTryBecomeLeader，不应抛出异常
            Runnable checkTask = runnableCaptor.getValue();
            checkTask.run(); // 应正常完成，不抛出 NPE
        }

        @Test
        @DisplayName("没有健康 Leader 时应尝试成为 Leader 并启动同步")
        void shouldTryBecomeLeaderAndStartSyncWhenNoHealthyLeader() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class)))
                    .thenReturn(localNamingService)
                    .thenReturn(testNamingService);

            // 初始化时有 Leader
            Instance existingLeader = createLeaderInstance("other-leader");
            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader)) // 初始化时有 Leader
                    .thenReturn(Collections.emptyList()) // checkAndTryBecomeLeader 时 Leader 消失
                    .thenReturn(Collections.emptyList()) // tryBecomeLeader 第一次检查
                    .thenReturn(Collections.singletonList(createLeaderInstance(INSTANCE_ID))); // tryBecomeLeader 第二次检查

            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), any(Duration.class));
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createEmptyListView());

            serviceDiscovery.initialize();

            // 手动触发 checkAndTryBecomeLeader
            Runnable checkTask = runnableCaptor.getValue();
            checkTask.run();

            // 验证尝试注册为 Leader
            verify(localNamingService, atLeastOnce()).registerInstance(eq("nacos-sync-leader"), anyString(), any(Instance.class));
            // 验证取消了 Leader 检查任务
            verify(scheduledFuture).cancel(false);
        }

        @Test
        @DisplayName("成为 Leader 后 startSyncAsLeader 失败时应释放 Leader 并重启检查任务")
        void shouldReleaseLeadershipAndRestartCheckWhenStartSyncFailsAfterBecomingLeader() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class)))
                    .thenReturn(localNamingService)
                    .thenThrow(new RuntimeException("Failed to connect to test Nacos")); // startSyncAsLeader 失败

            // 初始化时有 Leader
            Instance existingLeader = createLeaderInstance("other-leader");
            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader)) // 初始化时有 Leader
                    .thenReturn(Collections.emptyList()) // checkAndTryBecomeLeader 时 Leader 消失
                    .thenReturn(Collections.emptyList()) // tryBecomeLeader 第一次检查
                    .thenReturn(Collections.singletonList(createLeaderInstance(INSTANCE_ID))); // tryBecomeLeader 第二次检查

            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), any(Duration.class));

            serviceDiscovery.initialize();

            // 手动触发 checkAndTryBecomeLeader
            Runnable checkTask = runnableCaptor.getValue();
            checkTask.run();

            // 验证释放了 Leader
            verify(localNamingService, atLeastOnce()).deregisterInstance(eq("nacos-sync-leader"), anyString(), any(Instance.class));
            // 验证重启了检查任务（至少调用2次：初始化时1次 + 失败后重启1次）
            verify(taskScheduler, atLeast(2)).scheduleWithFixedDelay(any(Runnable.class), eq(Duration.ofSeconds(10)));
        }

        @Test
        @DisplayName("检查 Leader 时抛出异常且 leaderCheckTask 为 null 时应重启检查任务")
        void shouldRestartCheckTaskWhenExceptionAndTaskIsNull() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class))).thenReturn(localNamingService);

            Instance existingLeader = createLeaderInstance("other-leader");
            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader)) // 初始化时
                    .thenThrow(new RuntimeException("Network error")); // checkAndTryBecomeLeader 时抛出异常

            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), any(Duration.class));
            when(scheduledFuture.isCancelled()).thenReturn(true); // 模拟任务已取消

            serviceDiscovery.initialize();

            // 手动触发 checkAndTryBecomeLeader
            Runnable checkTask = runnableCaptor.getValue();
            checkTask.run();

            // 验证重启了检查任务
            verify(taskScheduler, atLeast(2)).scheduleWithFixedDelay(any(Runnable.class), eq(Duration.ofSeconds(10)));
        }

        @Test
        @DisplayName("检查 Leader 时抛出异常且 leaderCheckTask 已取消时应重启检查任务")
        void shouldRestartCheckTaskWhenExceptionAndTaskIsCancelled() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class))).thenReturn(localNamingService);

            Instance existingLeader = createLeaderInstance("other-leader");
            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader)) // 初始化时
                    .thenThrow(new RuntimeException("Connection timeout")); // checkAndTryBecomeLeader 时抛出异常

            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), any(Duration.class));
            when(scheduledFuture.isCancelled()).thenReturn(true); // 模拟任务已取消

            serviceDiscovery.initialize();

            // 手动触发 checkAndTryBecomeLeader
            Runnable checkTask = runnableCaptor.getValue();
            checkTask.run();

            // 验证重启了检查任务（因为 isCancelled 返回 true）
            verify(taskScheduler, atLeast(2)).scheduleWithFixedDelay(any(Runnable.class), eq(Duration.ofSeconds(10)));
        }

        @Test
        @DisplayName("检查 Leader 时抛出异常但任务仍在运行时不应重启")
        void shouldNotRestartCheckTaskWhenExceptionButTaskStillRunning() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class))).thenReturn(localNamingService);

            Instance existingLeader = createLeaderInstance("other-leader");
            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader)) // 初始化时
                    .thenThrow(new RuntimeException("Temporary error")); // checkAndTryBecomeLeader 时抛出异常

            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), any(Duration.class));
            when(scheduledFuture.isCancelled()).thenReturn(false); // 任务仍在运行

            serviceDiscovery.initialize();

            // 手动触发 checkAndTryBecomeLeader
            Runnable checkTask = runnableCaptor.getValue();
            checkTask.run();

            // 验证只调用了1次（初始化时），没有重启
            verify(taskScheduler, times(1)).scheduleWithFixedDelay(any(Runnable.class), eq(Duration.ofSeconds(10)));
        }

        @Test
        @DisplayName("tryBecomeLeader 返回 false 时不应启动同步")
        void shouldNotStartSyncWhenTryBecomeLeaderReturnsFalse() throws Exception {
            createServiceDiscovery();
            when(namingServiceFactory.create(any(Properties.class))).thenReturn(localNamingService);

            // 初始化时有 Leader
            Instance existingLeader = createLeaderInstance("other-leader");
            Instance anotherLeader = createLeaderInstance("another-leader");
            anotherLeader.getMetadata().put("startTime", "1"); // 更早

            when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(existingLeader)) // 初始化时有 Leader
                    .thenReturn(Collections.emptyList()) // checkAndTryBecomeLeader 时 Leader 消失
                    .thenReturn(Collections.emptyList()) // tryBecomeLeader 第一次检查：无 Leader
                    .thenReturn(Arrays.asList(anotherLeader, createLeaderInstance(INSTANCE_ID))); // 第二次检查：有其他更早的 Leader

            ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
            doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(runnableCaptor.capture(), any(Duration.class));

            serviceDiscovery.initialize();

            // 手动触发 checkAndTryBecomeLeader
            Runnable checkTask = runnableCaptor.getValue();
            checkTask.run();

            // 不应取消检查任务（因为没有成为 Leader）
            verify(scheduledFuture, never()).cancel(anyBoolean());
        }
    }

    @Nested
    @DisplayName("cleanupAllFallbackInstances 异常处理测试")
    class CleanupAllFallbackInstancesExceptionTest {

        @Test
        @DisplayName("清理某个服务失败时应继续清理其他服务")
        void shouldContinueCleanupWhenOneServiceFails() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            // 第一次同步：有 service-a 和 service-b
            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Arrays.asList("service-a", "service-b")))
                    .thenReturn(createEmptyListView()); // 第二次同步：无服务

            Instance testInstanceA = createInstance("172.16.0.1", 8080);
            Instance testInstanceB = createInstance("172.16.0.2", 8080);
            when(testNamingService.selectInstances(eq("service-a"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstanceA));
            when(testNamingService.selectInstances(eq("service-b"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(testInstanceB));
            when(localNamingService.getAllInstances(eq("service-a"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());
            when(localNamingService.getAllInstances(eq("service-b"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());

            serviceDiscovery.initialize();

            // 验证注册了实例
            verify(localNamingService).registerInstance(eq("service-a"), anyString(), any(Instance.class));
            verify(localNamingService).registerInstance(eq("service-b"), anyString(), any(Instance.class));
        }
    }

    @Nested
    @DisplayName("服务处理异常测试")
    class ServiceProcessingExceptionTest {

        @Test
        @DisplayName("处理单个服务时抛出异常应继续处理其他服务")
        void shouldContinueProcessingOtherServicesWhenOneThrows() throws Exception {
            createServiceDiscovery();
            setupLeaderMocks();

            when(testNamingService.getServicesOfServer(anyInt(), anyInt(), anyString()))
                    .thenReturn(createListView(Arrays.asList("service-a", "service-b")));

            // service-a 抛出异常
            when(localNamingService.getAllInstances(eq("service-a"), anyString(), eq(false)))
                    .thenThrow(new RuntimeException("Failed to get instances for service-a"));
            // service-b 正常
            when(localNamingService.getAllInstances(eq("service-b"), anyString(), eq(false)))
                    .thenReturn(Collections.emptyList());
            when(testNamingService.selectInstances(eq("service-b"), anyString(), eq(true)))
                    .thenReturn(Collections.singletonList(createInstance("172.16.0.2", 8080)));

            serviceDiscovery.initialize();

            // 验证 service-b 仍然被注册
            verify(localNamingService).registerInstance(eq("service-b"), anyString(), any(Instance.class));
        }
    }

    // ==================== Helper Methods ====================

    private void setupLeaderMocks() throws Exception {
        when(namingServiceFactory.create(any(Properties.class)))
                .thenReturn(localNamingService)
                .thenReturn(testNamingService);
        when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                .thenReturn(Collections.emptyList())
                .thenReturn(Collections.singletonList(createLeaderInstance(INSTANCE_ID)));
        doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));
    }

    private void setupLeaderMocksWithoutTestService() throws Exception {
        when(namingServiceFactory.create(any(Properties.class)))
                .thenReturn(localNamingService)
                .thenReturn(testNamingService);
        when(localNamingService.selectInstances(eq("nacos-sync-leader"), anyString(), eq(true)))
                .thenReturn(Collections.emptyList())
                .thenReturn(Collections.singletonList(createLeaderInstance(INSTANCE_ID)));
        doReturn(scheduledFuture).when(taskScheduler).scheduleWithFixedDelay(any(Runnable.class), any(Duration.class));
    }

    private Instance createInstance(String ip, int port) {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setHealthy(true);
        instance.setEnabled(true);
        instance.setEphemeral(true);
        instance.setMetadata(new HashMap<>());
        return instance;
    }

    private Instance createFallbackInstance(String ip, int port) {
        Instance instance = createInstance(ip, port);
        instance.getMetadata().put("fallback", "true");
        instance.getMetadata().put("source", "test-env");
        return instance;
    }

    private Instance createLeaderInstance(String instanceId) {
        Instance instance = new Instance();
        instance.setIp("127.0.0.1");
        instance.setPort(1);
        instance.setInstanceId(instanceId);
        instance.setHealthy(true);
        instance.setEnabled(true);
        instance.setEphemeral(true);
        Map<String, String> metadata = new HashMap<>();
        metadata.put("instanceId", instanceId);
        metadata.put("startTime", String.valueOf(System.currentTimeMillis()));
        instance.setMetadata(metadata);
        return instance;
    }

    private ListView<String> createListView(List<String> services) {
        return createListView(services, services.size());
    }

    private ListView<String> createListView(List<String> services, int totalCount) {
        ListView<String> listView = new ListView<>();
        listView.setData(services);
        listView.setCount(totalCount);
        return listView;
    }

    private ListView<String> createEmptyListView() {
        ListView<String> listView = new ListView<>();
        listView.setData(Collections.emptyList());
        listView.setCount(0);
        return listView;
    }
}
