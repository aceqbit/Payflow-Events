package io.payflow.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class UuidV7Test {
    @Test
    void hasVersion7VariantAndTimestamp() {
        var u = UuidV7.next(1_700_000_000_000L);
        assertThat(u.version()).isEqualTo(7);
        assertThat(u.variant()).isEqualTo(2);
        assertThat(u.getMostSignificantBits() >>> 16).isEqualTo(1_700_000_000_000L);
    }

    @Test
    void timeOrderedAcrossMilliseconds() {
        var a = UuidV7.next(1_000L).toString();
        var b = UuidV7.next(2_000L).toString();
        assertThat(a).isLessThan(b);
    }
}
