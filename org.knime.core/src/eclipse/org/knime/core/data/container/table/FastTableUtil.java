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
 *   May 9, 2020 (dietzc): created
 */
package org.knime.core.data.container.table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.knime.core.data.container.RowContainerFactory;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.table.store.chunk.TableChunkReaderConfig;
import org.knime.core.node.InvalidSettingsException;

/**
 * {@link RowContainerFactory} for fast tables.
 *
 * @author Christian Dietz, KNIME GmbH
 */
class FastTableUtil {

    private FastTableUtil() {
    }

    /**
     * Create an instance of a type represented by a string.
     *
     * @param <I> type of instance.
     * @param type fully qualified class name
     * @return instance of type
     * @throws InvalidSettingsException if object can't be instantiated
     */
    public static <I> I createInstance(final String type) throws InvalidSettingsException {
        try {
            @SuppressWarnings("unchecked")
            final I instance = (I)Class.forName(type).newInstance();
            return instance;
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException ex) {
            throw new InvalidSettingsException("Couldn't instantiate object of type " + type + ".", ex);
        }
    }

    // TODO efficiency
    public static TableChunkReaderConfig wrap(final TableFilter filter) {
        return new TableChunkReaderConfig() {
            @Override
            public int[] getColumnIndices() {
                Optional<Set<Integer>> materializeColumnIndices = filter.getMaterializeColumnIndices();
                final int[] selected;
                if (materializeColumnIndices.isPresent()) {
                    final List<Integer> indices = new ArrayList<>(materializeColumnIndices.get());
                    Collections.sort(indices);
                    selected = new int[indices.size()];
                    for (int i = 0; i < selected.length; i++) {
                        selected[i] = indices.get(i);
                    }
                } else {
                    selected = null;
                }
                return selected;
            }
        };
    }

    //    public static boolean supports(final DataTableSpec spec) {
    //        for (final DataColumnSpec colSpec : spec) {
    //            final DataType type = colSpec.getType();
    //            // TODO check for column blah
    //            if (!TableStoreAdapterRegistry.hasAdapter(type)) {
    //                final Class<? extends DataCell> cellClass = type.getCellClass();
    //                // we can serialize all DataCells which are not blobs, collections and filestores.
    //                if (BlobDataCell.class.isAssignableFrom(cellClass) //
    //                    || FileStoreCell.class.isAssignableFrom(cellClass) //
    //                    || type.isCollectionType()) {
    //                    return false;
    //                }
    //            }
    //        }
    //        return true;
    //    }

    //    private static DomainRowBatchStore createDelegate(final ColumnSpec<?>[] types, final File dest) {
    //        return new DomainRowBatchStore(RowBatchStoreUtils.cache(FACTORY.create(types, dest, new TableStoreConfig() {
    //
    //            @Override
    //            public int getInitialChunkSize() {
    //                // TODO make configurable. from where? constant? cache?
    //                return 8_00_000;
    //            }
    //        })), new DomainCalculationConfig() {
    //            @Override
    //            public int[] getDomainEnabledIndices() {
    //                // TODO efficiency for wide tables ... of course...e.g. implement (isEnabled(int idx)).
    //                // Don't compute domain for row key
    //                final int[] enabled = new int[types.length - 1];
    //                for (int i = 0; i < enabled.length; i++) {
    //                    enabled[i] = i + 1;
    //                }
    //                return enabled;
    //            }
    //        });
    //    }
}
