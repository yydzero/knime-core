package org.knime.core.node.gen.property.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

@SuppressWarnings("javadoc")
@Target({ElementType.FIELD})
public @interface Id {

    String value();

}
