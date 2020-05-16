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

import java.util.HashMap;
import java.util.Map;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.table.store.ColumnChunk;
import org.knime.core.data.table.store.ColumnChunkSpec;
import org.knime.core.data.table.store.types.BooleanChunk;
import org.knime.core.data.table.store.types.DoubleChunk;
import org.knime.core.data.table.store.types.IntChunk;
import org.knime.core.data.table.store.types.LongChunk;
import org.knime.core.data.table.store.types.StringChunk;

class LegacyMappings {
    private LegacyMappings() {
    }

    static final Map<DataType, LegacyMappingInfo<?>> PRIMITIVE_MAPPINGS = new HashMap<>();

    static {
        // TODO Extension point
        PRIMITIVE_MAPPINGS.put(IntCell.TYPE, new IntTypeMappingInfo());
        PRIMITIVE_MAPPINGS.put(DoubleCell.TYPE, new DoubleTypeMappingInfo());
        PRIMITIVE_MAPPINGS.put(StringCell.TYPE, new StringTypeMappingInfo());
        PRIMITIVE_MAPPINGS.put(LongCell.TYPE, new LongTypeMappingInfo());
        PRIMITIVE_MAPPINGS.put(BooleanCell.TYPE, new BooleanTypeMappingInfo());
    }

    interface LegacyDataCellMapper<C extends ColumnChunk> {
        void set(int index, DataCell source, C dest);

        DataCell get(int index, C source);
    }

    static interface LegacyMappingInfo<C extends ColumnChunk> {

        public Class<? extends DataCell> getCellType();

        public LegacyDataCellMapper<C> getMapper();

        public ColumnChunkSpec<C> getColumnChunkSpec();

    }

    static final class BooleanTypeMappingInfo implements LegacyMappingInfo<BooleanChunk> {
        @Override
        public Class<? extends DataCell> getCellType() {
            return BooleanCell.class;
        }

        @Override
        public LegacyDataCellMapper<BooleanChunk> getMapper() {
            return new LegacyBooleanCellMapper();
        }

        @Override
        public ColumnChunkSpec<BooleanChunk> getColumnChunkSpec() {
            return new BooleanChunk.BooleanChunkSpec();
        }

        static class LegacyBooleanCellMapper implements LegacyDataCellMapper<BooleanChunk> {
            @Override
            public void set(final int index, final DataCell source, final BooleanChunk dest) {
                dest.setBoolean(index, ((BooleanCell)source).getBooleanValue());
            }

            @Override
            public DataCell get(final int index, final BooleanChunk source) {
                return source.getBoolean(index) ? BooleanCell.TRUE : BooleanCell.FALSE;
            }
        }
    }

    static final class DoubleTypeMappingInfo implements LegacyMappingInfo<DoubleChunk> {

        @Override
        public Class<? extends DataCell> getCellType() {
            return DoubleCell.class;
        }

        @Override
        public LegacyDataCellMapper<DoubleChunk> getMapper() {
            return new LegacyDoubleCellMapper();
        }

        @Override
        public ColumnChunkSpec<DoubleChunk> getColumnChunkSpec() {
            return new DoubleChunk.DoubleChunkSpec();
        }

        static class LegacyDoubleCellMapper implements LegacyDataCellMapper<DoubleChunk> {
            @Override
            public void set(final int index, final DataCell source, final DoubleChunk dest) {
                dest.setDouble(index, ((DoubleCell)source).getDoubleValue());
            }

            @Override
            public DataCell get(final int index, final DoubleChunk source) {
                return new DoubleCell(source.getDouble(index));
            }
        }

    }

    static final class IntTypeMappingInfo implements LegacyMappingInfo<IntChunk> {

        @Override
        public Class<? extends DataCell> getCellType() {
            return IntCell.class;
        }

        @Override
        public LegacyDataCellMapper<IntChunk> getMapper() {
            return new LegacyIntCellMapper();
        }

        @Override
        public ColumnChunkSpec<IntChunk> getColumnChunkSpec() {
            return new IntChunk.IntChunkSpec();
        }

        static class LegacyIntCellMapper implements LegacyDataCellMapper<IntChunk> {
            @Override
            public void set(final int index, final DataCell source, final IntChunk dest) {
                dest.setInt(index, ((IntCell)source).getIntValue());
            }

            @Override
            public DataCell get(final int index, final IntChunk source) {
                return new IntCell(source.getInt(index));
            }
        }

    }

    static final class LongTypeMappingInfo implements LegacyMappingInfo<LongChunk> {

        @Override
        public Class<? extends DataCell> getCellType() {
            return LongCell.class;
        }

        @Override
        public LegacyDataCellMapper<LongChunk> getMapper() {
            return new LegacyLongCellMapper();
        }

        @Override
        public ColumnChunkSpec<LongChunk> getColumnChunkSpec() {
            return new LongChunk.LongChunkSpec();
        }

        class LegacyLongCellMapper implements LegacyDataCellMapper<LongChunk> {
            @Override
            public void set(final int index, final DataCell source, final LongChunk dest) {
                dest.setLong(index, ((LongCell)source).getLongValue());
            }

            @Override
            public DataCell get(final int index, final LongChunk source) {
                return new LongCell(source.getLong(index));
            }
        }

    }

    static final class StringTypeMappingInfo implements LegacyMappingInfo<StringChunk> {
        @Override
        public Class<? extends DataCell> getCellType() {
            return StringCell.class;
        }

        @Override
        public LegacyDataCellMapper<StringChunk> getMapper() {
            return new LegacyStringCellMapper();
        }

        @Override
        public ColumnChunkSpec<StringChunk> getColumnChunkSpec() {
            return new StringChunk.StringChunkSpec(true);
        }

        class LegacyStringCellMapper implements LegacyDataCellMapper<StringChunk> {
            @Override
            public void set(final int index, final DataCell source, final StringChunk dest) {
                dest.setString(index, ((StringCell)source).getStringValue());
            }

            @Override
            public DataCell get(final int index, final StringChunk source) {
                return new StringCell(source.getString(index));
            }
        }
    }
}
