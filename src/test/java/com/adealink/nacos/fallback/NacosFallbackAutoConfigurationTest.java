package com.adealink.nacos.fallback;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

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
                    });
        }

        @Test
        @DisplayName("当 nacos.fallback.enabled 未设置时不应创建 Bean")
        void shouldNotCreateBeansWhenPropertyNotSet() {
            contextRunner
                    .run(context -> {
                        assertThat(context).doesNotHaveBean(NacosFallbackServiceDiscovery.class);
                    });
        }
    }

    @Nested
    @DisplayName("ServiceDiscovery Bean 测试")
    class ServiceDiscoveryBeanTest {

        @Test
        @DisplayName("应正确创建 NacosFallbackServiceDiscovery（内部调度器）")
        void shouldCreateServiceDiscoveryBeanWithInternalScheduler() {
            NacosFallbackAutoConfiguration config = new NacosFallbackAutoConfiguration();
            NacosFallbackProperties properties = new NacosFallbackProperties();
            properties.setEnabled(true);
            properties.setTestPublicIp("10.0.0.1");

            // 现在 nacosFallbackServiceDiscovery 内部创建调度器，不再需要外部传入
            NacosFallbackServiceDiscovery discovery = config.nacosFallbackServiceDiscovery(properties);

            assertThat(discovery).isNotNull();

            // 停止以清理内部调度器
            discovery.stop();
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
