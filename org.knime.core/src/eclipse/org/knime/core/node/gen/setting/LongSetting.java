package org.knime.core.node.gen.setting;

@SuppressWarnings({"javadoc", "hiding"})
public final class LongSetting implements Setting<Long> {

    private final Long value;

    public LongSetting(final Long value) {
        this.value = value;
    }

    @Override
    public Long getValue() {
        return value;
    }

}
