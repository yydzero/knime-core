package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@SuppressWarnings("javadoc")
@Target({ElementType.FIELD})
public @interface SinceVersion {

    /**
     * The version number at which this property has been introduced. If settings
     */
    int value() default 1;

}
