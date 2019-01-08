package org.knime.core.node.gen.setting;

@SuppressWarnings({"javadoc", "hiding"})
public final class IntegerSetting implements Setting<Integer> {

    private final Integer value;

    public IntegerSetting(final Integer value) {
        this.value = value;
    }

    @Override
    public Integer getValue() {
        return value;
    }

}
