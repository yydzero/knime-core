package org.knime.core.node.gen.setting;

@SuppressWarnings({"javadoc", "hiding"})
public final class ColumnSelectionSetting implements SelectionSetting<String> {

    private String value;

    private final String[] choices;

    public ColumnSelectionSetting(final String value, final String[] choices) {
        this.value = value;
        this.choices = choices;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String[] getChoices() {
        return choices;
    }

}
