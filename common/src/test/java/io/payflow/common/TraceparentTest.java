package io.payflow.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TraceparentTest {
    @Test
    void formatsW3cAndPadsShortIds() {
        assertThat(Traceparent.format("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", true))
                .isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        assertThat(Traceparent.format("abc", "1", false))
                .isEqualTo("00-00000000000000000000000000000abc-0000000000000001-00");
    }
}
