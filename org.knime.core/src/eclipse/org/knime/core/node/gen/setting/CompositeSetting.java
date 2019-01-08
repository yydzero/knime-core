package org.knime.core.node.gen.setting;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

@SuppressWarnings("javadoc")
public abstract class CompositeSetting<T extends CompositeSetting<T>> implements Setting<List<Setting<?>>> {

    /**
     * this class will hold methods like saveToNodeSettings and loadFromNodeSettings. It will also interface with the
     * frontend and provide methods such as exportAsJSON. We will also do all kinds of checks here, e.g., whether a
     * setting is null without being annotated as nullable, whether an IntegerSetting is valid according to its
     * IntegerConstraints, etc.
     */

    public CompositeSetting() {
    }

    protected abstract T self();

    @Override
    public List<Setting<?>> getValue() {
        List<Setting<?>> value = new LinkedList<Setting<?>>();
        for (Field field : self().getClass().getDeclaredFields()) {
            field.setAccessible(true);
            if (Setting.class.isAssignableFrom(field.getType())) {
                try {
                    value.add((Setting<?>)field.get(self()));
                } catch (IllegalArgumentException ex) {
                    ex.printStackTrace();
                    System.exit(-1);
                } catch (IllegalAccessException ex) {
                    ex.printStackTrace();
                    System.exit(-1);
                }
            }
        }
        return value;
    }

}
