/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 * -------------------------------------------------------------------
 *
 */
package org.knime.base.node.io.database;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.knime.base.util.flowvariable.FlowVariableProvider;
import org.knime.base.util.flowvariable.FlowVariableResolver;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseReaderConnection;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 *
 * @author Thomas Gabriel, University of Konstanz
 */
class DBReaderNodeModel extends NodeModel implements FlowVariableProvider {
    protected final DatabaseQueryConnectionSettings m_settings = new DatabaseQueryConnectionSettings();

    private DataTableSpec m_lastSpec = null;

    /**
     * Creates a new model with the given number (and types!) of input and output types.
     *
     * @param inPortTypes an array of non-null in-port types
     * @param outPortTypes an array of non-null out-port types
     */
    protected DBReaderNodeModel(final PortType[] inPortTypes, final PortType[] outPortTypes) {
        super(inPortTypes, outPortTypes);
    }

    /**
     * Creates a new database reader with one data out-port.
     * @param ins number data input ports
     * @param outs number data output ports
     */
    DBReaderNodeModel(final int ins, final int outs) {
        super(ins, outs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inData,
            final ExecutionContext exec)
            throws CanceledExecutionException, Exception {
        exec.setProgress("Opening database connection...");
        final DatabaseReaderConnection load = new DatabaseReaderConnection(null);
        try {
            exec.setProgress("Reading data from database...");
            final BufferedDataTable result = getResultTable(exec, inData, load);
            setLastSpec(result.getDataTableSpec());
            return new BufferedDataTable[]{result};
        } catch (CanceledExecutionException cee) {
            throw cee;
        } catch (Exception e) {
            setLastSpec(null);
            throw e;
        }
    }

    /**
     * @param exec {@link ExecutionContext} to create the table
     * @param inData
     * @param load {@link DatabaseReaderConnection}
     * @return the result table for the
     * @throws CanceledExecutionException
     * @throws SQLException
     * @throws InvalidSettingsException
     */
    protected BufferedDataTable getResultTable(final ExecutionContext exec, final PortObject[] inData,
        final DatabaseReaderConnection load)
        throws CanceledExecutionException, SQLException, InvalidSettingsException {
        String query = parseQuery(m_settings.getQuery());
        DatabaseQueryConnectionSettings connSettings;
        if ((inData[getNrInPorts() - 1] instanceof DatabaseConnectionPortObject)) {
            DatabaseConnectionPortObject dbObj = (DatabaseConnectionPortObject)inData[getNrInPorts() - 1];

            connSettings =
                new DatabaseQueryConnectionSettings(dbObj.getConnectionSettings(getCredentialsProvider()), query);
        } else {
            connSettings = new DatabaseQueryConnectionSettings(m_settings, query);
        }
        load.setDBQueryConnection(new DatabaseQueryConnectionSettings(connSettings, query));
        CredentialsProvider cp = getCredentialsProvider();
        final BufferedDataTable result = load.createTable(exec, cp);
        return result;
    }

    private String parseQuery(final String query) {
        return FlowVariableResolver.parse(query, this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        // empty: don't reset m_lastSpec that is only touched when the actual
        // settings in the node dialog have changed.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException {
        File specFile = null;
        specFile = new File(nodeInternDir, "spec.xml");
        if (!specFile.exists()) {
            IOException ioe = new IOException("Spec file (\""
                    + specFile.getAbsolutePath() + "\") does not exist "
                    + "(node may have been saved by an older version!)");
            throw ioe;
        }
        NodeSettingsRO specSett =
            NodeSettings.loadFromXML(new FileInputStream(specFile));
        try {
            setLastSpec(DataTableSpec.load(specSett));
        } catch (InvalidSettingsException ise) {
            IOException ioe = new IOException("Could not read output spec.");
            ioe.initCause(ise);
            throw ioe;
        }
    }

    /**
     * @param spec the {@link DataTableSpec} of the result table to cache
     */
    protected void setLastSpec(final DataTableSpec spec) {
        m_lastSpec = spec;
    }

    /**
     * @return the cached result spec
     */
    protected DataTableSpec getLastSpec() {
        return m_lastSpec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException {
        final DataTableSpec lastSpec = getLastSpec();
        assert (lastSpec != null) : "Spec must not be null!";
        NodeSettings specSett = new NodeSettings("spec.xml");
        lastSpec.save(specSett);
        File specFile = new File(nodeInternDir, "spec.xml");
        specSett.saveToXML(new FileOutputStream(specFile));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs)
            throws InvalidSettingsException {
        final DataTableSpec lastSpec = getLastSpec();
        if (lastSpec != null) {
            return new DataTableSpec[]{lastSpec};
        }
        try {
            if ((m_settings.getCredentialName() == null)
                && ((m_settings.getUserName(getCredentialsProvider()) == null) || m_settings.getUserName(
                    getCredentialsProvider()).isEmpty())) {
                throw new InvalidSettingsException("No credentials or username for authentication given");
            }

            if ((m_settings.getQuery() == null) || m_settings.getQuery().isEmpty()) {
                throw new InvalidSettingsException("No query configured.");
            }
            if (!m_settings.getValidateQuery()) {
                return new DataTableSpec[] {null};
            }
            final DatabaseReaderConnection load = new DatabaseReaderConnection(null);
            DataTableSpec resultSpec = getResultSpec(inSpecs, load);
            setLastSpec(resultSpec);
            return new DataTableSpec[]{resultSpec};
        } catch (InvalidSettingsException e) {
            setLastSpec(null);
            throw e;
        } catch (SQLException ex) {
            setLastSpec(null);
            Throwable cause = ExceptionUtils.getRootCause(ex);
            if (cause == null) {
                cause = ex;
            }

            throw new InvalidSettingsException("Could not determine table spec from database query: "
                + cause.getMessage(), ex);
        }
    }

    /**
     * @param inSpecs input spec
     * @param load {@link DatabaseReaderConnection} to use
     * @return the {@link DataTableSpec} of the result table
     * @throws InvalidSettingsException if the connection settings are invalid
     * @throws SQLException if the query is invalid
     */
    protected DataTableSpec getResultSpec(final PortObjectSpec[] inSpecs, final DatabaseReaderConnection load)
        throws InvalidSettingsException, SQLException {
        String query = parseQuery(m_settings.getQuery());
        DatabaseQueryConnectionSettings connSettings;
        if ((inSpecs.length > getNrInPorts() - 1)
            && (inSpecs[getNrInPorts() - 1] instanceof DatabaseConnectionPortObjectSpec)) {
            DatabaseConnectionPortObjectSpec connSpec =
                (DatabaseConnectionPortObjectSpec)inSpecs[getNrInPorts() - 1];
            connSettings = new DatabaseQueryConnectionSettings(
                connSpec.getConnectionSettings(getCredentialsProvider()), query);
        } else {
            connSettings = new DatabaseQueryConnectionSettings(m_settings, query);
        }
        load.setDBQueryConnection(connSettings);
        final DataTableSpec resultSpec = load.getDataTableSpec(getCredentialsProvider());
        return resultSpec;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        String query = settings.getString(
                DatabaseConnectionSettings.CFG_STATEMENT);
        if (query != null && query.contains(
                DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER)) {
            throw new InvalidSettingsException(
                    "Database table place holder ("
                    + DatabaseQueryConnectionSettings.TABLE_PLACEHOLDER
                    + ") not replaced.");
        }

        DatabaseQueryConnectionSettings s = new DatabaseQueryConnectionSettings();
        s.validateConnection(settings, getCredentialsProvider());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        boolean settingsChanged = m_settings.loadValidatedConnection(settings, getCredentialsProvider());

        if (settingsChanged || (m_settings.getQuery() == null) || m_settings.getQuery().isEmpty()) {
            setLastSpec(null);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveConnection(settings);
    }

    /**
     * @param newQuery the new query to set
     */
    final void setQuery(final String newQuery) {
        m_settings.setQuery(newQuery);
    }

    /**
     * @return current query
     */
    final String getQuery() {
        return m_settings.getQuery();
    }
}
