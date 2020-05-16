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
 *   May 17, 2020 (dietzc): created
 */
package org.knime.core.data.container.table;

import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.UnmaterializedCell;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.table.ReadTable;
import org.knime.core.data.table.TableReadCursor;
import org.knime.core.data.table.store.chunk.TableChunkReaderConfig;

import gnu.trove.map.hash.TIntIntHashMap;

/**
 *
 * @author dietzc
 */
final class TableStoreIterators {
    private TableStoreIterators() {
    }

    static class EmptyRowIterator extends CloseableRowIterator {

        private final int m_numCells;

        private final TableReadCursor m_cursor;

        private final RowKeyReadValue m_rowKeyValue;

        public EmptyRowIterator(final ReadTable table) {
            m_numCells = table.getNumColumns() - 1;
            m_cursor = table.newCursor();
            m_rowKeyValue = m_cursor.get(0);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            try {
                m_cursor.close();
            } catch (Exception ex) {
                // TODO
                throw new RuntimeException(ex);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return m_cursor.canFwd();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            return new CompletelyUnmaterializedRow(m_rowKeyValue, m_numCells);
        }

        static class CompletelyUnmaterializedRow implements DataRow {

            private static final UnmaterializedCell INSTANCE = UnmaterializedCell.getInstance();

            private final RowKeyReadValue m_rowKeyValue;

            private final int m_numCells;

            CompletelyUnmaterializedRow(final RowKeyReadValue rowKeyValue, final int numCells) {
                m_numCells = numCells;
                m_rowKeyValue = rowKeyValue;
            }

            // TODO in case of filtering, over what columns do I actually iterate?
            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {

                    int i = 0;

                    @Override
                    public boolean hasNext() {
                        return i < m_numCells;
                    }

                    @Override
                    public DataCell next() {
                        i++;
                        return INSTANCE;
                    }
                };
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int getNumCells() {
                return m_numCells;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public RowKey getKey() {
                return m_rowKeyValue.getRowKey();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public DataCell getCell(final int index) {
                return INSTANCE;
            }
        }

    }

    static class EmptyRowIteratorNoKey extends CloseableRowIterator {

        private final DataRow m_rowInstance;

        private final long m_size;

        private long m_index;

        public EmptyRowIteratorNoKey(final int numCells, final long size) {
            m_size = size;
            m_rowInstance = new CompletelyUnmaterializedRow(numCells);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void close() {
            // No op
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return m_index < m_size;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            m_index++;
            return m_rowInstance;
        }

        static class CompletelyUnmaterializedRow implements DataRow {

            private static final UnmaterializedCell INSTANCE = UnmaterializedCell.getInstance();

            private final int m_numCells;

            CompletelyUnmaterializedRow(final int numCells) {
                m_numCells = numCells;
            }

            // TODO in case of filtering, over what columns do I actually iterate?
            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {

                    int i = 0;

                    @Override
                    public boolean hasNext() {
                        return i < m_numCells;
                    }

                    @Override
                    public DataCell next() {
                        i++;
                        return INSTANCE;
                    }
                };
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int getNumCells() {
                return m_numCells;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public RowKey getKey() {
                throw new IllegalStateException("RowKey requested but not available!");
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public DataCell getCell(final int index) {
                return INSTANCE;
            }
        }
    }

    // TODO shared abstract class with NoKey
    static class PartialRowIterator extends CloseableRowIterator {

        private final TableReadCursor m_cursor;

        private final RowKeyReadValue m_rowKeySupplier;

        private final DataCellReadValue[] m_values;

        private final TIntIntHashMap m_indexMap;

        private final int m_numCells;

        public PartialRowIterator(final ReadTable table, final TableChunkReaderConfig config) {
            m_cursor = table.newCursor(config);
            int[] selected = config.getColumnIndices();
            m_rowKeySupplier = m_cursor.get(0);
            // TODO use selected
            m_values = new DataCellReadValue[table.getNumColumns()];
            for (int i = 0; i < selected.length; i++) {
                m_values[selected[i]] = m_cursor.get(selected[i] + 1);
            }
            m_numCells = table.getNumColumns() - 1;

            // TODO wrong wrong wrong
            m_indexMap = new TIntIntHashMap(selected.length, 0.5f, -1, -1);
            for (int i = 0; i < selected.length; i++) {
                m_indexMap.put(selected[i], i);
            }
        }

        @Override
        public boolean hasNext() {
            return m_cursor.canFwd();
        }

        @Override
        public DataRow next() {
            m_cursor.fwd();
            return new PartialFastTableDataRow(m_rowKeySupplier, m_values, m_indexMap, m_numCells);
        }

        @Override
        public void close() {
            try {
                m_cursor.close();
            } catch (Exception e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        // TODO split in with rowkey and without rowkey
        static class PartialFastTableDataRow implements DataRow {

            private static final UnmaterializedCell INSTANCE = UnmaterializedCell.getInstance();

            // TODO
            private final RowKeyReadValue m_rowKeyValue;

            private final DataCellReadValue[] m_values;

            private final TIntIntHashMap m_indexMap;

            private final int m_numCells;

            public PartialFastTableDataRow(final RowKeyReadValue rowKeySupplier, final DataCellReadValue[] values,
                final TIntIntHashMap indexMap, final int numCells) {
                m_rowKeyValue = rowKeySupplier;
                m_values = values;
                m_numCells = numCells;
                m_indexMap = indexMap;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {
                    int idx = 1;

                    @Override
                    public boolean hasNext() {
                        return idx < m_numCells;
                    }

                    @Override
                    public DataCell next() {
                        return getCell(idx++);
                    }
                };
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int getNumCells() {
                return m_numCells;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public RowKey getKey() {
                return m_rowKeyValue.getRowKey();
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public DataCell getCell(final int index) {
                // TODO this is the cost of the contract, that we keep column indices
                // TODO check if this is really faster than Map<Integer,Supplier>
                final int i = m_indexMap.get(index);
                if (i == -1) {
                    return INSTANCE;
                } else {
                    return m_values[index + 1].getDataCell();
                }
            }
        }
    }

    static class PartialRowIteratorNoKey extends CloseableRowIterator {

        private final TableReadCursor m_cursor;

        private final DataCellReadValue[] m_values;

        private final TIntIntHashMap m_indexMap;

        private final int m_numCells;

        public PartialRowIteratorNoKey(final ReadTable table, final TableChunkReaderConfig config) {
            m_cursor = table.newCursor(config);
            int[] selected = config.getColumnIndices();
            // TODO use selected
            m_values = new DataCellReadValue[table.getNumColumns()];
            for (int i = 0; i < selected.length; i++) {
                m_values[selected[i]] = m_cursor.get(selected[i]);
            }
            m_numCells = table.getNumColumns();

            // TODO only initialize required suppliers in case of partial table.
            // TODO do mapping once. we can use the same mapping for each reader.
            // TODO which mapping to use? output spec from knime or columntypes spec of store?

            // TODO wrong wrong wrong
            m_indexMap = new TIntIntHashMap(selected.length, 0.5f, -1, -1);
            for (int i = 0; i < selected.length; i++) {
                m_indexMap.put(selected[i], i);
            }
        }

        @Override
        public boolean hasNext() {
            return m_cursor.canFwd();
        }

        @Override
        public DataRow next() {
            m_cursor.fwd();
            return new PartialFastTableDataRow(m_values, m_indexMap, m_numCells);
        }

        @Override
        public void close() {
            try {
                m_cursor.close();
            } catch (Exception e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        // TODO split in with rowkey and without rowkey
        static class PartialFastTableDataRow implements DataRow {

            private static final UnmaterializedCell INSTANCE = UnmaterializedCell.getInstance();

            private final DataCellReadValue[] m_producers;

            private final TIntIntHashMap m_indexMap;

            private final int m_numCells;

            public PartialFastTableDataRow(final DataCellReadValue[] selectedSuppliers, final TIntIntHashMap indexMap,
                final int numCells) {
                m_producers = selectedSuppliers;
                m_numCells = numCells;
                m_indexMap = indexMap;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {
                    int idx = 0;

                    @Override
                    public boolean hasNext() {
                        return idx < m_numCells;
                    }

                    @Override
                    public DataCell next() {
                        return getCell(idx++);
                    }
                };
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public int getNumCells() {
                return m_numCells;
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public RowKey getKey() {
                throw new IllegalStateException("RowKey requested, but not part of table. Implementation error!");
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public DataCell getCell(final int index) {
                // TODO this is the cost of the contract, that we keep column indices
                // TODO check if this is really faster than Map<Integer,Supplier>
                final int i = m_indexMap.get(index);
                if (i == -1) {
                    return INSTANCE;
                } else {
                    return m_producers[index].getDataCell();
                }
            }
        }
    }

    static class TableStoreRowIterator extends CloseableRowIterator {

        private final TableReadCursor m_cursor;

        // TODO
        private final RowKeyReadValue m_rowKeyValue;

        private final DataCellReadValue[] m_values;

        TableStoreRowIterator(final ReadTable table) {
            m_cursor = table.newCursor();
            m_values = new DataCellReadValue[table.getNumColumns()];
            for (int i = 1; i < m_values.length; i++) {
                m_values[i - 1] = m_cursor.get(i);
            }
            m_rowKeyValue = m_cursor.get(0);
        }

        @Override
        public boolean hasNext() {
            return m_cursor.canFwd();
        }

        @Override
        public DataRow next() {
            m_cursor.fwd();
            return new FastTableDataRow(m_rowKeyValue, m_values);
        }

        @Override
        public void close() {
            try {
                m_cursor.close();
            } catch (Exception e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        static class FastTableDataRow implements DataRow {

            private final RowKeyReadValue m_rowKeyValue;

            private final DataCellReadValue[] m_producer;

            public FastTableDataRow(final RowKeyReadValue rowKeyValue, final DataCellReadValue[] producer) {
                m_rowKeyValue = rowKeyValue;
                m_producer = producer;
            }

            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {
                    int idx = 1;

                    @Override
                    public boolean hasNext() {
                        return idx < m_producer.length;
                    }

                    @Override
                    public DataCell next() {
                        return getCell(idx++);
                    }
                };
            }

            @Override
            public int getNumCells() {
                return m_producer.length;
            }

            @Override
            public RowKey getKey() {
                return m_rowKeyValue.getRowKey();
            }

            @Override
            public DataCell getCell(final int index) {
                return m_producer[index + 1].getDataCell();
            }
        }
    }

    static class TableStoreRowIteratorNoKey extends CloseableRowIterator {

        private final TableReadCursor m_cursor;

        private final DataCellReadValue[] m_values;

        TableStoreRowIteratorNoKey(final ReadTable table) {
            m_cursor = table.newCursor();
            m_values = new DataCellReadValue[table.getNumColumns()];
            for (int i = 0; i < m_values.length; i++) {
                m_values[i] = m_cursor.get(i);
            }
        }

        @Override
        public boolean hasNext() {
            return m_cursor.canFwd();
        }

        @Override
        public DataRow next() {
            m_cursor.fwd();
            return new FastTableDataRowNoKey(m_values);
        }

        @Override
        public void close() {
            try {
                m_cursor.close();
            } catch (Exception e) {
                // TODO
                throw new RuntimeException(e);
            }
        }

        static class FastTableDataRowNoKey implements DataRow {

            private final DataCellReadValue[] m_values;

            public FastTableDataRowNoKey(final DataCellReadValue[] producer) {
                m_values = producer;
            }

            @Override
            public Iterator<DataCell> iterator() {
                return new Iterator<DataCell>() {
                    int idx = 0;

                    @Override
                    public boolean hasNext() {
                        return idx < m_values.length;
                    }

                    @Override
                    public DataCell next() {
                        return getCell(idx++);
                    }
                };
            }

            @Override
            public int getNumCells() {
                return m_values.length;
            }

            @Override
            public RowKey getKey() {
                throw new IllegalStateException("RowKey requested, but not part of table. Implementation error!");
            }

            @Override
            public DataCell getCell(final int index) {
                return m_values[index].getDataCell();
            }
        }
    }
}
