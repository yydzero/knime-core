package org.knime.core.data.container.table;

import org.knime.core.data.DataCell;
import org.knime.core.data.table.ReadValue;

/**
 * TODO
 *
 * @author dietzc
 */
public interface DataCellReadValue extends ReadValue {
    /**
     * @return datacell
     */
    DataCell getDataCell();
}