package org.knime.core.node.gen.setting;

@SuppressWarnings("javadoc")
public interface SelectionSetting<T> extends Setting<T> {

    T[] getChoices();

}
