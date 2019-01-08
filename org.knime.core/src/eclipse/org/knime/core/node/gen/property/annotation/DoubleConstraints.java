package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@SuppressWarnings("javadoc")
@Target({ElementType.FIELD})
public @interface DoubleConstraints {

    /**
     * The minimum value that this property can be set to.
     */
    double min() default Double.MIN_VALUE;

    /**
     * The maximum value that this property can be set to.
     */
    double max() default Double.MAX_VALUE;

}
