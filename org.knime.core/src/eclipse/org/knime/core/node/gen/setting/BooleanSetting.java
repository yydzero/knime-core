package org.knime.core.node.gen.setting;

@SuppressWarnings({"javadoc", "hiding"})
public final class BooleanSetting implements Setting<Boolean> {

    private Boolean value;

    public BooleanSetting(final Boolean value) {
        this.value = value;
    }

    @Override
    public Boolean getValue() {
        return value;
    }

}
