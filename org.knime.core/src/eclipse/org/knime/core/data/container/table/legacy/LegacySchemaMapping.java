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
 *   May 23, 2020 (dietzc): created
 */
package org.knime.core.data.container.table.legacy;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataCellSerializer;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataTypeRegistry;
import org.knime.core.data.container.table.TableSchemaMapping;
import org.knime.core.data.container.table.legacy.LegacyMappings.LegacyMappingInfo;
import org.knime.core.data.table.ColumnSpec;
import org.knime.core.data.table.TableSchema;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.knime.core.util.Pair;

/**
 * Legacy mapping implementation.
 *
 * @author Christian Dietz, KNIME GmbH, Konstanz
 */
class LegacySchemaMapping implements TableSchemaMapping {
    private static final String TABLE_SCHEMA_MAPPING_ROWKEY = "TABLE_SCHEMA_MAPPING_ROWKEY";

    private DataCellSerMap m_map;

    private TableSchema m_schema;

    private boolean m_isRowKey;

    LegacySchemaMapping() {
        m_map = new DataCellSerMap();
    }

    LegacySchemaMapping(final DataTableSpec spec, final boolean isRowKey) {
        this();
        m_schema = map(spec, isRowKey);
        m_isRowKey = isRowKey;
    }

    @Override
    public TableSchema getSchema() {
        return m_schema;
    }

    @Override
    public void saveTo(final ConfigWO settings) {
        m_map.saveTo(settings);
        settings.addBoolean(TABLE_SCHEMA_MAPPING_ROWKEY, m_isRowKey);
    }

    @Override
    public void loadFrom(final DataTableSpec spec, final ConfigRO settings) {
        try {
            m_map.loadFrom(settings);
            m_schema = map(spec, settings.getBoolean(TABLE_SCHEMA_MAPPING_ROWKEY));
        } catch (InvalidSettingsException ex) {
        }
    }

    private TableSchema map(final DataTableSpec spec, final boolean isRowKey) {
        int offset = (isRowKey ? 1 : 0);
        ColumnSpec<?>[] columnSpec = new ColumnSpec[spec.getNumColumns() + (isRowKey ? 1 : 0)];
        if (isRowKey) {
            columnSpec[0] = new LegacyRowKeyColumnSpec();
        }
        for (int i = 0; i < spec.getNumColumns(); i++) {
            final LegacyMappingInfo<?> mappingInfo =
                LegacyMappings.PRIMITIVE_MAPPINGS.get(spec.getColumnSpec(i).getType());
            if (mappingInfo != null) {
                columnSpec[i + offset] = new LegacyDataCellColumnSpec<>(m_map, mappingInfo);
            } else {
                throw new IllegalStateException("Shouldn't happen");
            }
        }

        return new TableSchema() {
            @Override
            public int getNumColumns() {
                return columnSpec.length;
            }

            @Override
            public ColumnSpec<?> getColumnSpec(final int idx) {
                return columnSpec[idx];
            }
        };
    }

    static final class DataCellSerMap {

        private static final String TABLE_SCHEMA_MAPPING_TYPES = "TABLE_SCHEMA_MAPPING_TYPES";

        private final static DataTypeRegistry REGISTRY = DataTypeRegistry.getInstance();

        private final Map<Class<? extends DataCell>, Byte> m_byType;

        private final Map<Byte, Class<? extends DataCell>> m_byIdx;

        private byte m_currIdx = Byte.MIN_VALUE;

        public DataCellSerMap() {
            m_byType = new TreeMap<>();
            m_byIdx = new TreeMap<>();
        }

        /**
         * @param cell
         * @return pair of index and data cell serializer.
         */
        public Pair<Byte, DataCellSerializer<DataCell>> getSerializer(final DataCell cell) {
            final Class<? extends DataCell> type = cell.getClass();
            final Byte idx = m_byType.get(type);
            final Pair<Byte, DataCellSerializer<DataCell>> res;
            if (idx == null) {
                if (m_currIdx == Byte.MAX_VALUE) {
                    throw new IllegalStateException("Too many cell implementations!");
                } else {
                    final DataCellSerializer<DataCell> serializer = REGISTRY.getSerializer(type).get();
                    m_byType.put(type, m_currIdx++);
                    m_byIdx.put(m_currIdx, type);
                    res = new Pair<>(m_currIdx, serializer);
                }
            } else {
                res = new Pair<>(idx, REGISTRY.getSerializer(type).get());
            }
            return res;
        }

        /**
         * @param index of {@link DataCellSerializer}
         * @return associated {@link DataCellSerializer}
         */
        public DataCellSerializer<DataCell> getSerializerByIdx(final byte index) {
            // TODO OK?
            return REGISTRY.getSerializer(m_byIdx.get(index)).get();
        }

        public void saveTo(final ConfigWO settings) {
            for (Entry<Class<? extends DataCell>, Byte> entry : m_byType.entrySet()) {
                final String[] cells = new String[m_byIdx.size()];
                for (int i = 0; i < cells.length; i++) {
                    cells[i] = entry.getKey().getName();
                }
                settings.addStringArray(TABLE_SCHEMA_MAPPING_TYPES, cells);
            }
        }

        public void loadFrom(final ConfigRO settings) {
            try {
                final String[] cells = settings.getStringArray(TABLE_SCHEMA_MAPPING_TYPES);
                byte idx = Byte.MIN_VALUE;
                for (final String cell : cells) {
                    Class<? extends DataCell> type = REGISTRY.getCellClass(cell).get();
                    m_byIdx.put(idx, type);
                    m_byType.put(type, idx);
                    idx++;
                }
            } catch (InvalidSettingsException ex) {
            }
        }

    }
}
