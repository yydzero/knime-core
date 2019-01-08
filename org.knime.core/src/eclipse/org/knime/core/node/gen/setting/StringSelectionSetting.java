package org.knime.core.node.gen.setting;

@SuppressWarnings({"javadoc", "hiding"})
public final class StringSelectionSetting implements SelectionSetting<String> {

    private final String value;

    private final String[] choices;

    public StringSelectionSetting(final String value, final String[] choices) {
        this.value = value;
        this.choices = choices;
    }

    @Override
    public String[] getChoices() {
        return choices;
    }

    @Override
    public String getValue() {
        return value;
    }

}
