package org.knime.core.node.gen.setting;

@SuppressWarnings({"javadoc", "hiding"})
public final class EnumSelectionSetting<E extends Enum<E>> implements SelectionSetting<E> {

    private final E value;

    private final E[] choices;

    public EnumSelectionSetting(final E value, final E[] choices) {
        this.value = value;
        this.choices = choices;
    }

    @Override
    public E[] getChoices() {
        return choices;
    }

    @Override
    public E getValue() {
        return value;
    }

}
