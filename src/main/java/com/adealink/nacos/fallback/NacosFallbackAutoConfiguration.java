package com.adealink.nacos.fallback;

import com.alibaba.cloud.nacos.ConditionalOnNacosDiscoveryEnabled;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.ConditionalOnDiscoveryEnabled;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Nacos Fallback 自动配置
 * <p>
 * 定期同步远程环境服务到本地 Nacos，适用于本地开发环境。
 * </p>
 *
 * @author suyihang
 * @since 1.0.0
 */
@Slf4j
@Configuration
@ConditionalOnClass(name = "com.alibaba.nacos.api.naming.NamingService")
@ConditionalOnDiscoveryEnabled
@ConditionalOnNacosDiscoveryEnabled
@ConditionalOnProperty(value = "nacos.fallback.enabled", havingValue = "true")
@EnableConfigurationProperties(NacosFallbackProperties.class)
public class NacosFallbackAutoConfiguration {

    /**
     * 创建 Nacos Fallback 服务同步组件
     * <p>
     * 通过 initMethod 和 destroyMethod 管理生命周期：
     * <ul>
     *   <li>start(): 异步启动同步任务</li>
     *   <li>stop(): 释放资源，清理 fallback 实例</li>
     * </ul>
     * <p>
     * 内部使用专用的任务调度器，不暴露为 Spring Bean，
     * 避免影响应用中 @Scheduled 注解的调度行为。
     */
    @Bean(initMethod = "start", destroyMethod = "stop")
    @ConditionalOnMissingBean
    public NacosFallbackServiceDiscovery nacosFallbackServiceDiscovery(NacosFallbackProperties properties) {
        log.info("Initializing Nacos fallback sync service");
        log.info("  Local Nacos: {}", properties.getLocalServerAddr());
        log.info("  Remote Nacos: {}", properties.getTestServerAddr());
        log.info("  Sync interval: {} seconds", properties.getSyncIntervalSeconds());

        return new NacosFallbackServiceDiscovery(properties);
    }
}
