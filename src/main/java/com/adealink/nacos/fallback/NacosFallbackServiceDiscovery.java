package com.adealink.nacos.fallback;

import com.alibaba.nacos.api.naming.NamingFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ListView;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import com.alibaba.nacos.api.exception.NacosException;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

/**
 * Nacos Fallback 服务同步
 * <p>
 * 定期从测试环境 Nacos 拉取服务并注册到本地 Nacos，
 * 支持 Leader 选举、增量更新、自动清理等特性。
 * </p>
 *
 * @author suyihang
 * @since 1.0.0
 */
@Slf4j
public class NacosFallbackServiceDiscovery {

    private static final int PAGE_SIZE = 100;

    private final NacosFallbackProperties properties;
    private final NamingServiceFactory namingServiceFactory;
    private final ThreadPoolTaskScheduler taskScheduler;
    private final boolean ownedScheduler; // 标记调度器是否由本类创建，用于判断是否需要关闭
    private volatile NamingService testNamingService;
    private volatile NamingService localNamingService;
    private volatile boolean initialized = false;

    // Leader 选举相关
    private volatile boolean isLeader = false;
    private final String instanceId;
    private Instance leaderInstance;

    // 调度任务引用，用于停止时取消
    private volatile ScheduledFuture<?> syncTask;
    private volatile ScheduledFuture<?> leaderCheckTask;

    // 已同步的服务名集合，用于清理不再存在的服务
    private final Set<String> syncedServices = new HashSet<>();

    public NacosFallbackServiceDiscovery(NacosFallbackProperties properties) {
        this(properties, createInternalScheduler(), new DefaultNamingServiceFactory(), UUID.randomUUID().toString(), true);
    }

    NacosFallbackServiceDiscovery(NacosFallbackProperties properties,
                                  ThreadPoolTaskScheduler taskScheduler,
                                  NamingServiceFactory namingServiceFactory,
                                  String instanceId,
                                  boolean ownedScheduler) {
        this.properties = properties;
        this.taskScheduler = taskScheduler;
        this.namingServiceFactory = namingServiceFactory;
        this.instanceId = instanceId;
        this.ownedScheduler = ownedScheduler;
    }

    /**
     * 创建内部专用的任务调度器
     * <p>
     * 不暴露为 Spring Bean，避免影响应用中的 @Scheduled 注解
     */
    private static ThreadPoolTaskScheduler createInternalScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("nacos-fallback-");
        scheduler.setDaemon(true);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(10);
        scheduler.initialize();
        return scheduler;
    }

    /**
     * 启动同步任务
     */
    public void start() {
        if (!properties.isEnabled()) {
            log.info("Nacos fallback sync is disabled");
            return;
        }

        // 异步初始化,避免阻塞启动
        taskScheduler.schedule(this::initialize, Instant.now());
    }

    /**
     * 初始化 Nacos 客户端
     */
    void initialize() {
        try {
            log.info("Initializing Nacos fallback sync service, instanceId: {}", instanceId);

            // 初始化本地 Nacos 客户端（Leader 选举需要）
            Properties localProps = new Properties();
            localProps.put("serverAddr", properties.getLocalServerAddr());
            if (StringUtils.hasText(properties.getLocalNamespace())) {
                localProps.put("namespace", properties.getLocalNamespace());
            }
            localNamingService = namingServiceFactory.create(localProps);
            log.info("Connected to local Nacos: {}", properties.getLocalServerAddr());

            // 尝试成为 Leader
            if (tryBecomeLeader()) {
                log.info("This instance became the sync leader");
                isLeader = true;
                try {
                    startSyncAsLeader();
                } catch (Exception e) {
                    // startSyncAsLeader 失败时，释放 Leader 并启动 Leader 检查
                    log.error("Failed to start sync as leader, releasing leadership", e);
                    releaseLeadership();
                    startLeaderCheckTask();
                }
            } else {
                log.info("Another instance is the sync leader, this instance will monitor leader health");
                isLeader = false;
                startLeaderCheckTask();
            }

        } catch (Exception e) {
            log.error("Failed to initialize Nacos fallback sync service", e);
            initialized = false;
            scheduleRetry();
        }
    }

    /**
     * 启动 Leader 健康检查任务
     */
    private void startLeaderCheckTask() {
        long checkIntervalSeconds = properties.getLeaderCheckIntervalSeconds();
        leaderCheckTask = taskScheduler.scheduleWithFixedDelay(
                this::checkAndTryBecomeLeader,
                Duration.ofSeconds(checkIntervalSeconds)
        );
        log.info("Leader health check started, interval: {} seconds", checkIntervalSeconds);
    }

    /**
     * 作为 Leader 启动同步任务
     * 抽取的公共方法，避免代码重复
     */
    private void startSyncAsLeader() throws Exception {
        // 初始化测试环境 Nacos 客户端（只有 Leader 需要）
        Properties testProps = new Properties();
        testProps.put("serverAddr", properties.getTestServerAddr());
        if (StringUtils.hasText(properties.getTestNamespace())) {
            testProps.put("namespace", properties.getTestNamespace());
        }
        testNamingService = namingServiceFactory.create(testProps);
        log.info("Connected to test Nacos: {}", properties.getTestServerAddr());

        initialized = true;

        // 立即执行一次同步
        syncServices();

        // 启动定期同步任务
        long syncIntervalSeconds = properties.getSyncIntervalSeconds();
        syncTask = taskScheduler.scheduleWithFixedDelay(
                this::syncServices,
                Duration.ofSeconds(syncIntervalSeconds)
        );

        log.info("Nacos fallback sync started, interval: {} seconds", syncIntervalSeconds);
    }

    /**
     * 同步服务
     */
    private void syncServices() {
        if (!initialized) {
            return;
        }

        try {
            log.debug("Starting service sync from test to local Nacos");

            // 获取测试环境所有服务（分页获取）
            List<String> testServices = getAllServicesWithPagination();
            if (CollectionUtils.isEmpty(testServices)) {
                log.debug("No services found in test environment");
                // 清理所有 fallback 实例
                cleanupAllFallbackInstances();
                return;
            }

            log.info("Found {} services in test environment", testServices.size());

            Set<String> currentSyncedServices = new HashSet<>();
            int syncCount = 0;
            for (String serviceName : testServices) {
                try {
                    Map<String, List<Instance>> categorizedInstances = getInstancesAndCategorize(serviceName);
                    List<Instance> nativeInstances = categorizedInstances.get("native");
                    List<Instance> fallbackInstances = categorizedInstances.get("fallback");

                    // 如果本地 Nacos 存在非 fallback 的原生实例,则跳过该服务的同步,本地优先
                    if (!CollectionUtils.isEmpty(nativeInstances)) {
                        log.debug("Service {} has native instances in local Nacos, skipping fallback sync.", serviceName);
                        // 如果有原生实例，清理该服务的 fallback 实例
                        if (!CollectionUtils.isEmpty(fallbackInstances)) {
                            log.info("Cleaning up {} fallback instances for service {} (native instances exist).", fallbackInstances.size(), serviceName);
                            deregisterInstances(serviceName, fallbackInstances);
                        }
                        continue;
                    }

                    // 先拉取成功再替换，避免拉取失败导致服务短暂下线
                    if (syncServiceInstances(serviceName, fallbackInstances)) {
                        syncCount++;
                        currentSyncedServices.add(serviceName);
                    } else {
                        // 拉取失败，保留旧实例，将服务加入跟踪集合以便下次重试
                        if (!CollectionUtils.isEmpty(fallbackInstances)) {
                            log.info("Keeping {} old fallback instances for service {} (sync failed)", fallbackInstances.size(), serviceName);
                            currentSyncedServices.add(serviceName);
                        }
                    }
                } catch (Exception e) {
                    log.error("Error processing service {} during sync", serviceName, e);
                }
            }

            // 清理不再存在于测试环境的服务的 fallback 实例
            Set<String> failedCleanupServices = cleanupStaleServices(testServices, currentSyncedServices);

            // 更新已同步服务集合（保留清理失败的服务以便下次重试）
            synchronized (syncedServices) {
                syncedServices.clear();
                syncedServices.addAll(currentSyncedServices);
                syncedServices.addAll(failedCleanupServices);
            }

            if (syncCount > 0) {
                log.info("Synced {} services from test to local Nacos", syncCount);
            } else {
                log.debug("No new services to sync");
            }

        } catch (Exception e) {
            log.error("Error during service sync", e);
        }
    }

    /**
     * 分页获取所有服务列表
     */
    private List<String> getAllServicesWithPagination() throws NacosException {
        List<String> allServices = new ArrayList<>();
        int pageNo = 1;

        while (true) {
            ListView<String> servicesPage = testNamingService.getServicesOfServer(pageNo, PAGE_SIZE, properties.getTestGroup());
            List<String> services = servicesPage.getData();

            if (CollectionUtils.isEmpty(services)) {
                break;
            }

            allServices.addAll(services);

            // 检查是否还有更多数据
            int totalCount = servicesPage.getCount();
            if (allServices.size() >= totalCount || services.size() < PAGE_SIZE) {
                break;
            }

            pageNo++;
        }

        return allServices;
    }

    /**
     * 清理不再存在于测试环境的服务的 fallback 实例
     * @return 清理失败的服务名集合，需要保留在跟踪集合中以便下次重试
     */
    private Set<String> cleanupStaleServices(List<String> currentTestServices, Set<String> currentSyncedServices) {
        Set<String> staleServices;
        synchronized (syncedServices) {
            staleServices = new HashSet<>(syncedServices);
        }
        // 转换为 Set 提高 removeAll 性能
        staleServices.removeAll(new HashSet<>(currentTestServices));
        staleServices.removeAll(currentSyncedServices);

        Set<String> failedServices = new HashSet<>();
        for (String staleService : staleServices) {
            try {
                Map<String, List<Instance>> categorizedInstances = getInstancesAndCategorize(staleService);
                List<Instance> fallbackInstances = categorizedInstances.get("fallback");
                if (!CollectionUtils.isEmpty(fallbackInstances)) {
                    log.info("Cleaning up {} stale fallback instances for service {}", fallbackInstances.size(), staleService);
                    deregisterInstances(staleService, fallbackInstances);
                }
            } catch (Exception e) {
                log.error("Error cleaning up stale service {}, will retry next sync", staleService, e);
                failedServices.add(staleService);
            }
        }
        return failedServices;
    }

    /**
     * 清理所有 fallback 实例（当测试环境无服务时）
     * 只有成功清理的服务才从跟踪集合中移除，失败的会在下次同步时重试
     */
    private void cleanupAllFallbackInstances() {
        Set<String> servicesToClean;
        synchronized (syncedServices) {
            servicesToClean = new HashSet<>(syncedServices);
        }

        Set<String> successfullyCleaned = new HashSet<>();
        for (String serviceName : servicesToClean) {
            try {
                Map<String, List<Instance>> categorizedInstances = getInstancesAndCategorize(serviceName);
                List<Instance> fallbackInstances = categorizedInstances.get("fallback");
                if (!CollectionUtils.isEmpty(fallbackInstances)) {
                    log.info("Cleaning up {} fallback instances for service {} (no services in test env)", fallbackInstances.size(), serviceName);
                    deregisterInstances(serviceName, fallbackInstances);
                }
                // 只有成功处理的才标记为已清理
                successfullyCleaned.add(serviceName);
            } catch (Exception e) {
                log.error("Error cleaning up fallback instances for service {}, will retry next sync", serviceName, e);
            }
        }

        // 只移除成功清理的服务
        synchronized (syncedServices) {
            syncedServices.removeAll(successfullyCleaned);
        }
    }

    /**
     * 同步单个服务的所有实例
     * 使用增量更新策略，只更新有变化的实例，避免服务抖动
     *
     * @param serviceName 服务名
     * @param oldFallbackInstances 旧的 fallback 实例
     * @return true 如果同步成功
     */
    private boolean syncServiceInstances(String serviceName, List<Instance> oldFallbackInstances) {
        try {
            // 从测试环境获取服务实例
            List<Instance> testInstances = testNamingService.selectInstances(
                    serviceName,
                    properties.getTestGroup(),
                    true
            );

            if (CollectionUtils.isEmpty(testInstances)) {
                log.debug("No healthy instances for service {} in test environment", serviceName);
                return false;
            }

            // 构建本地 fallback 实例的 key 集合 (ip:port)
            Map<String, Instance> oldInstanceMap = new HashMap<>();
            if (!CollectionUtils.isEmpty(oldFallbackInstances)) {
                for (Instance inst : oldFallbackInstances) {
                    oldInstanceMap.put(inst.getIp() + ":" + inst.getPort(), inst);
                }
            }

            // 构建测试环境实例的 key 集合 (重写后的 ip:port)
            Map<String, Instance> newInstanceMap = new HashMap<>();
            for (Instance inst : testInstances) {
                String rewrittenIp = rewriteIpIfNeeded(inst.getIp());
                newInstanceMap.put(rewrittenIp + ":" + inst.getPort(), inst);
            }

            // 计算需要删除的实例（本地有但测试环境没有）
            List<Instance> toRemove = new ArrayList<>();
            for (Map.Entry<String, Instance> entry : oldInstanceMap.entrySet()) {
                if (!newInstanceMap.containsKey(entry.getKey())) {
                    toRemove.add(entry.getValue());
                }
            }

            // 计算需要新增的实例（测试环境有但本地没有）
            List<Instance> toAdd = new ArrayList<>();
            for (Map.Entry<String, Instance> entry : newInstanceMap.entrySet()) {
                if (!oldInstanceMap.containsKey(entry.getKey())) {
                    toAdd.add(entry.getValue());
                }
            }

            // 如果没有变化，跳过
            if (toRemove.isEmpty() && toAdd.isEmpty()) {
                log.debug("Service {} instances unchanged, skipping sync", serviceName);
                return true;
            }

            // 先注册新实例，再删除旧实例，减少服务不可用时间
            if (!toAdd.isEmpty()) {
                log.info("Adding {} new fallback instances for service {}", toAdd.size(), serviceName);
                for (Instance instance : toAdd) {
                    registerToLocal(serviceName, instance);
                }
            }

            if (!toRemove.isEmpty()) {
                log.info("Removing {} stale fallback instances for service {}", toRemove.size(), serviceName);
                deregisterInstances(serviceName, toRemove);
            }

            return true;

        } catch (Exception e) {
            log.error("Failed to sync service {}", serviceName, e);
            return false;
        }
    }

    /**
     * 将实例注册到本地 Nacos
     */
    private void registerToLocal(String serviceName, Instance instance) {
        try {
            // 创建新的实例
            Instance localInstance = new Instance();

            // IP 重写:将内网 IP 替换为公网 IP
            String originalIp = instance.getIp();
            String rewrittenIp = rewriteIpIfNeeded(originalIp);
            localInstance.setIp(rewrittenIp);

            localInstance.setPort(instance.getPort());
            localInstance.setServiceName(serviceName);
            localInstance.setClusterName(instance.getClusterName());
            localInstance.setWeight(instance.getWeight());
            localInstance.setHealthy(instance.isHealthy());
            localInstance.setEnabled(instance.isEnabled());
            // 强制设置为临时实例，防止持久脏数据
            localInstance.setEphemeral(true);

            // 复制元数据并添加标记
            Map<String, String> metadata = new HashMap<>();
            if (instance.getMetadata() != null) {
                metadata.putAll(instance.getMetadata());
            }
            metadata.put("fallback", "true");
            metadata.put("source", "test-env");
            metadata.put("original-ip", originalIp);
            metadata.put("synced-at", String.valueOf(System.currentTimeMillis()));
            localInstance.setMetadata(metadata);

            // 注册到本地 Nacos
            localNamingService.registerInstance(serviceName, properties.getLocalGroup(), localInstance);

            if (!originalIp.equals(rewrittenIp)) {
                log.info("Registered service {} instance to local Nacos: {}:{} (original IP: {})",
                        serviceName, rewrittenIp, instance.getPort(), originalIp);
            } else {
                log.info("Registered service {} instance to local Nacos: {}:{}",
                        serviceName, rewrittenIp, instance.getPort());
            }

        } catch (Exception e) {
            log.error("Failed to register instance of service {} to local Nacos", serviceName, e);
        }
    }

    /**
     * 重写 IP 地址
     */
    private String rewriteIpIfNeeded(String ip) {
        if (!StringUtils.hasText(ip) || !StringUtils.hasText(properties.getTestPublicIp())) {
            return ip;
        }

        // 检查私网前缀是否配置，为空则不重写
        String privateIpPrefix = properties.getTestPrivateIpPrefix();
        if (!StringUtils.hasText(privateIpPrefix)) {
            return ip;
        }

        // 检查是否是内网 IP
        if (ip.startsWith(privateIpPrefix)) {
            return properties.getTestPublicIp();
        }

        return ip;
    }

    /**
     * 获取指定服务在本地 Nacos 的所有实例,并分类为 fallback 和 native
     */
    private Map<String, List<Instance>> getInstancesAndCategorize(String serviceName) throws NacosException {
        // 使用 subscribe=false 避免建立订阅，只需要一次性快照
        List<Instance> allLocalInstances = localNamingService.getAllInstances(serviceName, properties.getLocalGroup(), false);
        List<Instance> fallbackInstances = new ArrayList<>();
        List<Instance> nativeInstances = new ArrayList<>();

        if (!CollectionUtils.isEmpty(allLocalInstances)) {
            for (Instance instance : allLocalInstances) {
                // 增加 metadata 判空保护
                Map<String, String> metadata = instance.getMetadata();
                if (metadata != null && "true".equals(metadata.get("fallback"))) {
                    fallbackInstances.add(instance);
                } else {
                    nativeInstances.add(instance);
                }
            }
        }

        Map<String, List<Instance>> categorizedInstances = new HashMap<>();
        categorizedInstances.put("fallback", fallbackInstances);
        categorizedInstances.put("native", nativeInstances);
        return categorizedInstances;
    }

    /**
     * 从本地 Nacos 注销指定服务的所有实例
     */
    private void deregisterInstances(String serviceName, List<Instance> instances) {
        if (CollectionUtils.isEmpty(instances)) {
            return;
        }
        for (Instance instance : instances) {
            try {
                localNamingService.deregisterInstance(serviceName, properties.getLocalGroup(), instance);
                log.info("Deregistered instance {}:{} of service {} from local Nacos (fallback cleanup)", instance.getIp(), instance.getPort(), serviceName);
            } catch (NacosException e) {
                log.error("Failed to deregister instance {}:{} of service {}", instance.getIp(), instance.getPort(), serviceName, e);
            }
        }
    }

    /**
     * 尝试成为 Leader
     * 通过在本地 Nacos 注册一个临时实例来实现
     *
     * @return true 如果成功成为 Leader
     */
    private boolean tryBecomeLeader() {
        try {
            String leaderServiceName = properties.getLeaderServiceName();

            // 检查是否已有 Leader
            List<Instance> existingLeaders = localNamingService.selectInstances(
                    leaderServiceName,
                    properties.getLocalGroup(),
                    true
            );

            if (!CollectionUtils.isEmpty(existingLeaders)) {
                // 已有健康的 Leader
                Instance existingLeader = existingLeaders.get(0);
                Map<String, String> metadata = existingLeader.getMetadata();
                String leaderId = metadata != null ? metadata.get("instanceId") : "unknown";
                log.info("Found existing leader: {}", leaderId);
                return false;
            }

            // 注册自己为 Leader
            leaderInstance = new Instance();
            leaderInstance.setIp("127.0.0.1");
            leaderInstance.setPort(1); // 使用端口 1（Nacos 要求端口 > 0）
            leaderInstance.setInstanceId(instanceId);
            leaderInstance.setEphemeral(true); // 临时实例，服务停止后自动注销
            leaderInstance.setHealthy(true);
            leaderInstance.setEnabled(true);

            Map<String, String> metadata = new HashMap<>();
            metadata.put("instanceId", instanceId);
            metadata.put("startTime", String.valueOf(System.currentTimeMillis()));
            leaderInstance.setMetadata(metadata);

            localNamingService.registerInstance(leaderServiceName, properties.getLocalGroup(), leaderInstance);
            log.info("Registered as sync leader with instanceId: {}", instanceId);

            // 短暂等待后再次检查，确保没有竞争条件
            Thread.sleep(properties.getLeaderElectionWaitMs());

            List<Instance> leaders = localNamingService.selectInstances(
                    leaderServiceName,
                    properties.getLocalGroup(),
                    true
            );

            // 检查自己是否是唯一的或者第一个 Leader
            if (!CollectionUtils.isEmpty(leaders)) {
                // 如果有多个实例，选择 startTime 最早的作为 Leader
                Instance firstLeader = leaders.stream()
                        .min(Comparator.comparingLong(i -> {
                            Map<String, String> m = i.getMetadata();
                            return Long.parseLong(m != null ? m.getOrDefault("startTime", "0") : "0");
                        }))
                        .get(); // leaders 非空，min 一定有值

                Map<String, String> firstLeaderMetadata = firstLeader.getMetadata();
                String firstLeaderId = firstLeaderMetadata != null ? firstLeaderMetadata.get("instanceId") : null;

                if (instanceId.equals(firstLeaderId)) {
                    return true;
                } else {
                    // 不是第一个，注销自己
                    log.info("Lost leader election to {}, deregistering self", firstLeaderId);
                    releaseLeadership();
                    return false;
                }
            }

            return true;

        } catch (Exception e) {
            log.error("Failed to try become leader", e);
            return false;
        }
    }

    /**
     * 检查 Leader 健康状态，如果 Leader 不存在则尝试成为 Leader
     */
    private void checkAndTryBecomeLeader() {
        if (isLeader) {
            return; // 已经是 Leader
        }

        try {
            String leaderServiceName = properties.getLeaderServiceName();
            List<Instance> leaders = localNamingService.selectInstances(
                    leaderServiceName,
                    properties.getLocalGroup(),
                    true
            );

            if (CollectionUtils.isEmpty(leaders)) {
                log.info("No healthy leader found, trying to become leader");

                if (tryBecomeLeader()) {
                    isLeader = true;
                    log.info("Successfully became the new sync leader");

                    // 取消 Leader 检查任务
                    if (leaderCheckTask != null) {
                        leaderCheckTask.cancel(false);
                        leaderCheckTask = null;
                    }

                    try {
                        startSyncAsLeader();
                        log.info("Nacos fallback sync started after leader election");
                    } catch (Exception e) {
                        // startSyncAsLeader 失败时，释放 Leader 并重新启动 Leader 检查
                        log.error("Failed to start sync after becoming leader, releasing leadership", e);
                        releaseLeadership();
                        startLeaderCheckTask();
                    }
                }
            } else {
                Map<String, String> leaderMetadata = leaders.get(0).getMetadata();
                String leaderId = leaderMetadata != null ? leaderMetadata.get("instanceId") : "unknown";
                log.debug("Leader is healthy: {}", leaderId);
            }

        } catch (Exception e) {
            log.error("Error checking leader health", e);
            // 异常时确保 Leader 检查任务仍在运行
            if (leaderCheckTask == null || leaderCheckTask.isCancelled()) {
                log.info("Restarting leader check task after error");
                startLeaderCheckTask();
            }
        }
    }

    /**
     * 释放 Leader 身份
     */
    private void releaseLeadership() {
        if (leaderInstance != null && localNamingService != null) {
            try {
                localNamingService.deregisterInstance(
                        properties.getLeaderServiceName(),
                        properties.getLocalGroup(),
                        leaderInstance
                );
                log.info("Released leadership, deregistered leader instance");
            } catch (Exception e) {
                log.error("Failed to deregister leader instance", e);
            }
            leaderInstance = null;
        }
        isLeader = false;
        initialized = false;
    }

    /**
     * 停止同步服务
     */
    public void stop() {
        try {
            log.info("Stopping Nacos fallback sync service");

            // 释放 Leader 身份
            if (isLeader) {
                releaseLeadership();
            }

            // 取消调度任务
            if (syncTask != null) {
                syncTask.cancel(false);
                syncTask = null;
            }
            if (leaderCheckTask != null) {
                leaderCheckTask.cancel(false);
                leaderCheckTask = null;
            }

            // 清理所有 fallback 实例
            if (localNamingService != null) {
                cleanupAllFallbackInstances();
            }

            // 关闭内部创建的调度器
            if (ownedScheduler && taskScheduler != null) {
                taskScheduler.shutdown();
            }

            log.info("Nacos fallback sync service stopped");
        } catch (Exception e) {
            log.error("Error stopping Nacos fallback sync service", e);
        }
    }

    private void scheduleRetry() {
        long delaySeconds = Math.max(5, properties.getSyncIntervalSeconds());
        try {
            taskScheduler.schedule(this::initialize, Instant.now().plusSeconds(delaySeconds));
            log.info("Scheduled retry for Nacos fallback sync initialization after {} seconds", delaySeconds);
        } catch (Exception scheduleException) {
            log.error("Failed to schedule retry for Nacos fallback sync service", scheduleException);
        }
    }

    // ==================== 内部接口和实现 ====================

    /**
     * NamingService 工厂接口，便于测试时 mock
     */
    interface NamingServiceFactory {
        NamingService create(Properties properties) throws Exception;
    }

    /**
     * 默认 NamingService 工厂实现
     */
    static class DefaultNamingServiceFactory implements NamingServiceFactory {
        @Override
        public NamingService create(Properties properties) throws Exception {
            return NamingFactory.createNamingService(properties);
        }
    }
}
