/*
 * ------------------------------------------------------------------------
 *
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
 *   May 1, 2020 (dietzc): created
 */
package org.knime.core.data.container.table.legacy;

import java.io.File;

import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.RowContainer;
import org.knime.core.data.container.table.DataCellWriteValue;
import org.knime.core.data.container.table.RowKeyWriteValue;
import org.knime.core.data.container.table.TableSchemaMapping;
import org.knime.core.data.table.TableUtils;
import org.knime.core.data.table.TableWriteCursor;
import org.knime.core.data.table.store.TableChunkStore;
import org.knime.core.data.table.store.TableChunkStoreFactory;

/**
 * TODO
 *
 * @author Christian Dietz, KNIME GmbH
 */
public class LegacyRowContainer implements RowContainer {

    private int m_size;

    private final RowKeyWriteValue m_rowKeyWriteValue;

    private final DataCellWriteValue[] m_values;

    private final int m_offset;

    private TableWriteCursor m_cursor;

    private DataTableSpec m_dataTableSpec;

    private ContainerTable m_table;

    private final int m_tableId;

    private final TableChunkStore m_store;

    private final TableSchemaMapping m_mapping;

    private final TableChunkStoreFactory m_factory;

    LegacyRowContainer(final int tableId, final DataTableSpec spec, final File file,
        final TableChunkStoreFactory factory, final boolean isRowKey) {
        // TODO Legacy
        m_mapping = new LegacySchemaMapping(spec, isRowKey);
        m_factory = factory;
        m_tableId = tableId;
        m_dataTableSpec = spec;

        // TODO THIS IS WEIRD. We should create a fully abstracted layer here and not mix and match layers.
        m_store = factory.createWriteStore(m_mapping.getSchema(), file);
        m_cursor = TableUtils.create(m_mapping.getSchema(), m_store).getCursor();

        m_values = new DataCellWriteValue[spec.getNumColumns()];
        for (int i = 0; i < m_values.length; i++) {
            m_values[i] = m_cursor.get(i + (isRowKey ? 1 : 0));
        }

        // TODO split into two implementations.
        if (isRowKey) {
            m_rowKeyWriteValue = m_cursor.get(0);
            m_offset = 1;
        } else {
            m_rowKeyWriteValue = null;
            m_offset = 0;
        }
    }

    @Override
    public void addRowToTable(final DataRow row) {
        m_cursor.fwd();
        if (m_rowKeyWriteValue != null) {
            m_rowKeyWriteValue.setRowKey(row.getKey());
        }
        for (int i = m_offset; i < m_values.length; i++) {
            m_values[i].setDataCell(row.getCell(i - m_offset));
        }
        m_size++;
    }

    @Override
    public void setMaxPossibleValues(final int maxPossibleValues) {
        // TODO
        throw new UnsupportedOperationException("nyi");
    }

    @Override
    public long size() {
        return m_size;
    }

    @Override
    public void close() {
        if (m_cursor != null) {
            try {
                m_cursor.close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        //        final DataColumnSpec[] colSpecs = new DataColumnSpec[m_sourceSpec.getNumColumns()];
        //        final DataColumnDomain[] domains = m_adapter.translate(m_store);
        //        for (int i = 0; i < colSpecs.length; i++) {
        //            final DataColumnSpecCreator specCreator = new DataColumnSpecCreator(m_sourceSpec.getColumnSpec(i));
        //            if (domains[i] != null) {
        //                specCreator.setDomain(domains[i]);
        //            }
        //            colSpecs[i] = specCreator.createSpec();
        //        }
        m_table = new LegacyTmpTableStoreTable(m_factory, m_tableId, m_dataTableSpec, m_store, m_mapping, m_size);
    }

    @Override
    public ContainerTable getTable() {
        if (m_table == null) {
            throw new IllegalStateException("GetTable can only be called after close().");
        }
        return m_table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        if (m_table != null) {
            m_table.clear();
        } else {
            try {
                m_cursor.close();
                m_store.close();
            } catch (Exception ex) {
                // TODO
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataTableSpec getTableSpec() {
        if (m_table != null) {
            return m_table.getDataTableSpec();
        } else {
            return m_dataTableSpec;
        }
    }
}
