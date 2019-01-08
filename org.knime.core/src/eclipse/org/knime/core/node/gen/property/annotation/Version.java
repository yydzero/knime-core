package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@SuppressWarnings("javadoc")
@Target({ElementType.TYPE})
public @interface Version {

    /**
     * The version number of this node's configuration. Can be increased when old properties are dropped or new
     * properties are added to a configuration.
     */
    int value() default 1;

}
