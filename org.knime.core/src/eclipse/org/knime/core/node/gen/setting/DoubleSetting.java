package org.knime.core.node.gen.setting;

@SuppressWarnings({"javadoc", "hiding"})
public final class DoubleSetting implements Setting<Double> {

    private Double value;

    public DoubleSetting(final Double value) {
        this.value = value;
    }

    @Override
    public Double getValue() {
        return value;
    }

}
