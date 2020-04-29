/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   12.09.2007 (Fabian Dill): created
 */
package org.knime.core.node.defaultnodesettings;

import java.text.MessageFormat;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;

/**
 * A settings model that manages a java enum type.
 * Enums can be more convenient than Strings because we can additional data or even logic to them.
 * Instead of having to serialize an object, the enum's ordinal can be stored and used to restore the enum object.<br/>
 *
 * The conversion from int back to an enum works like this:
 * <pre><code>
 * int ordinal = settings.getInt(m_configKey, m_defaultValue.ordinal());
 * T value = m_defaultValue.getDeclaringClass().getEnumConstants()[ordinal]
 * </code></pre>
 *
 * Example usage:
 * <pre><code>
 * enum Horizon { NORTH, EAST, SOUTH, WEST }
 * SettingsModelEnum&lt;Horizon&gt; model;
 * model.setValue(Horizon.NORTH);
 * </code></pre>
 *
 * @param <T> The enum type to manage in this SettingsModel.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 *
 */
public class SettingsModelEnum<T extends Enum<T>> extends SettingsModel {

    /** The string used to identify the value of this setting in a NodeSettings object. */
    private final String m_configKey;
    /** The current value of this setting. */
    private T m_value;
    /** The default value for this setting. Unlike {@link #m_value}, this can not be set to null. An instance
     * of the enum is necessary to get access to the enum's class (via declaringClass) see class docs. */
    private final T m_defaultValue;

    /**
     * @param configName the identifier the value is stored with in the {@link org.knime.core.node.NodeSettings} object.
     *            Must not be null or empty.
     * @param defaultValue the default value for the value of this settings model. The initial value is set to this
     *            value. Must not be null.
     * @throws IllegalArgumentException
     */
    public SettingsModelEnum(final String configName, final T defaultValue) {
        if ((configName == null) || "".equals(configName)) {
            throw new IllegalArgumentException("The configName must be a non-empty string");
        }
        m_configKey = configName;
        if(defaultValue == null) {
            // the value is needed to access the enum's class.
            throw new IllegalArgumentException("Default value must be provided.");
        }
        m_defaultValue = defaultValue;
        m_value = defaultValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        // use the default value, if the key is not present in the settings
        int ordinal = settings.getInt(m_configKey, m_defaultValue.ordinal());

        // convert back to enum instance
        T value = m_defaultValue.getDeclaringClass().getEnumConstants()[ordinal];

        setValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadSettingsForDialog(final NodeSettingsRO settings,
            final PortObjectSpec[] specs) throws NotConfigurableException {
        try {
            loadSettingsForModel(settings);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsForModel(final NodeSettingsWO settings) {
        // store only the ordinal of the enum instance
        settings.addInt(m_configKey, m_value.ordinal());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsForDialog(final NodeSettingsWO settings)
            throws InvalidSettingsException {
        saveSettingsForModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettingsForModel(final NodeSettingsRO settings)
        throws InvalidSettingsException {

        int ordinal = settings.getInt(m_configKey, m_defaultValue.ordinal());

        if (ordinal < 0) {
            throw new InvalidSettingsException(String.format(
                "Invalid value for enumeration setting %s. " + "Its ordinal value must not be negative, but is %s.",
                m_configKey, ordinal));
        }

        int maxAllowedOrdinal = m_defaultValue.getDeclaringClass().getEnumConstants().length - 1;
        if (ordinal > maxAllowedOrdinal) {
            throw new InvalidSettingsException(String.format(
                "Invalid value for enumeration setting %s. Its ordinal value must be ≤ %s, but is %s.",
                m_configKey, maxAllowedOrdinal, ordinal));
        }

    }

    /**
     * If the new value is different from the old value the listeners are notified.
     *
     * @param value the new value
     */
    public void setValue(final T value) {
        boolean notify = (value != m_value);

        m_value = value;

        if (notify) {
            notifyChangeListeners();
        }
    }

    /**
     * @return the stored long value
     */
    public T getValue() {
        return m_value;
    }

    String getEnumValueStr() {
        return m_value.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return MessageFormat.format("Enum Settings Model ({0}) = {1}", m_configKey, m_value.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getConfigName() {
        return m_configKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    protected SettingsModelEnum<T> createClone() {
        SettingsModelEnum<T> model = new SettingsModelEnum<>(m_configKey, m_defaultValue);
        return model;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getModelTypeID() {
        return "SM_enum";
    }

}
