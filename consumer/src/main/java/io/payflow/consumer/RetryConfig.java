package io.payflow.consumer;

import io.payflow.common.NonRetryableEventException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.kafka.retrytopic.DestinationTopic;
import org.springframework.kafka.retrytopic.RetryTopicComponentFactory;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationSupport;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.kafka.retrytopic.SuffixingRetryTopicNamesProviderFactory;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;

/**
 * Non-blocking retries (CLAUDE.md rule 7): a failing event leaves the main partition immediately and goes
 * payment.events -> payment.events-retry-5s -> payment.events-retry-1m -> payments.dlq. Three attempts in total,
 * backoff 5 s then 60 s. Deserialization and validation errors are not retryable and go straight to the DLQ.
 * Topics are never auto-created here (rule 8): topic-init owns them. The KafkaTemplate below is the framework's
 * dead-letter publisher, not a business-event producer.
 */
@Configuration
public class RetryConfig extends RetryTopicConfigurationSupport {
    public static final String MAIN_TOPIC = "payment.events";
    public static final String DLQ_TOPIC = "payments.dlq";
    static final long FIRST_DELAY_MS = 5_000;
    static final long SECOND_DELAY_MS = 60_000;

    /** Retry topics pause partitions until a record is due; the framework schedules the resume on this scheduler. */
    @Bean
    ThreadPoolTaskScheduler taskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("retry-resume-");
        return scheduler;
    }

    @Bean
    RetryTopicConfiguration payflowRetryTopics(KafkaTemplate<String, String> template) {
        return RetryTopicConfigurationBuilder.newInstance()
                .includeTopic(MAIN_TOPIC)
                .exponentialBackoff(FIRST_DELAY_MS, 12, SECOND_DELAY_MS)
                .maxAttempts(3)
                .autoCreateTopics(false, null, null)
                .setTopicSuffixingStrategy(TopicSuffixingStrategy.SUFFIX_WITH_DELAY_VALUE)
                .notRetryOn(NonRetryableEventException.class)
                .create(template);
    }

    @Override
    protected RetryTopicComponentFactory createComponentFactory() {
        return new RetryTopicComponentFactory() {
            @Override
            public RetryTopicNamesProviderFactory retryTopicNamesProviderFactory() {
                return new PayflowNames();
            }
        };
    }

    /** payment.events-retry-5s, payment.events-retry-1m and the DLT payments.dlq. */
    static final class PayflowNames extends SuffixingRetryTopicNamesProviderFactory {
        @Override
        public RetryTopicNamesProvider createRetryTopicNamesProvider(DestinationTopic.Properties props) {
            return new SuffixingRetryTopicNamesProvider(props) {
                @Override
                public String getTopicName(String topic) {
                    if (props.isDltTopic()) {
                        return DLQ_TOPIC;
                    }
                    if (props.isRetryTopic()) {
                        return topic + (props.delay() <= FIRST_DELAY_MS ? "-retry-5s" : "-retry-1m");
                    }
                    return super.getTopicName(topic);
                }
            };
        }
    }
}
