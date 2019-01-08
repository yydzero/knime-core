package org.knime.core.node.gen.setting;

@SuppressWarnings({"javadoc", "hiding"})
public final class StringSetting implements Setting<String> {

    private final String value;

    public StringSetting(final String value) {
        this.value = value;
    }

    @Override
    public String getValue() {
        return value;
    }

}
