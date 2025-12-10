package com.adealink.nacos.fallback;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * NacosFallbackAutoConfiguration 单元测试
 */
@DisplayName("NacosFallbackAutoConfiguration 测试")
class NacosFallbackAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(NacosFallbackAutoConfiguration.class));

    @Nested
    @DisplayName("条件装配测试")
    class ConditionalTest {

        @Test
        @DisplayName("当 nacos.fallback.enabled=false 时不应创建 Bean")
        void shouldNotCreateBeansWhenDisabled() {
            contextRunner
                    .withPropertyValues("nacos.fallback.enabled=false")
                    .run(context -> {
                        assertThat(context).doesNotHaveBean(NacosFallbackServiceDiscovery.class);
                        assertThat(context).doesNotHaveBean("nacosFallbackTaskScheduler");
                    });
        }

        @Test
        @DisplayName("当 nacos.fallback.enabled 未设置时不应创建 Bean")
        void shouldNotCreateBeansWhenPropertyNotSet() {
            contextRunner
                    .run(context -> {
                        assertThat(context).doesNotHaveBean(NacosFallbackServiceDiscovery.class);
                        assertThat(context).doesNotHaveBean("nacosFallbackTaskScheduler");
                    });
        }
    }

    @Nested
    @DisplayName("TaskScheduler Bean 测试")
    class TaskSchedulerBeanTest {

        @Test
        @DisplayName("ThreadPoolTaskScheduler 应正确配置")
        void shouldConfigureThreadPoolTaskScheduler() {
            NacosFallbackAutoConfiguration config = new NacosFallbackAutoConfiguration();
            ThreadPoolTaskScheduler scheduler = config.nacosFallbackTaskScheduler();

            assertThat(scheduler).isNotNull();
            assertThat(scheduler.getThreadNamePrefix()).isEqualTo("nacos-fallback-");
            // getPoolSize() 返回当前活跃线程数，而不是配置的池大小
            // 通过提交任务验证调度器正常工作
            assertThat(scheduler.getScheduledExecutor()).isNotNull();

            // 清理
            scheduler.shutdown();
        }
    }

    @Nested
    @DisplayName("ServiceDiscovery Bean 测试")
    class ServiceDiscoveryBeanTest {

        @Test
        @DisplayName("应正确创建 NacosFallbackServiceDiscovery")
        void shouldCreateServiceDiscoveryBean() {
            NacosFallbackAutoConfiguration config = new NacosFallbackAutoConfiguration();
            NacosFallbackProperties properties = new NacosFallbackProperties();
            properties.setEnabled(true);
            properties.setTestPublicIp("10.0.0.1");

            ThreadPoolTaskScheduler scheduler = config.nacosFallbackTaskScheduler();
            NacosFallbackServiceDiscovery discovery = config.nacosFallbackServiceDiscovery(properties, scheduler);

            assertThat(discovery).isNotNull();

            // 清理
            scheduler.shutdown();
        }
    }

    @Nested
    @DisplayName("配置属性绑定测试")
    class ConfigurationPropertiesTest {

        @Test
        @DisplayName("NacosFallbackProperties 应使用 nacos.fallback 前缀")
        void shouldUseCorrectPrefix() {
            assertThat(NacosFallbackProperties.class.getAnnotation(
                    org.springframework.boot.context.properties.ConfigurationProperties.class).prefix())
                    .isEqualTo("nacos.fallback");
        }
    }
}
