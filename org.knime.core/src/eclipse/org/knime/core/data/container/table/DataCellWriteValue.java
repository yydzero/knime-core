package org.knime.core.data.container.table;

import org.knime.core.data.DataCell;
import org.knime.core.data.table.WriteValue;

/**
 * TODO
 *
 * @author dietzc
 */
public interface DataCellWriteValue extends WriteValue {
    /**
     *
     * @param cell
     */
    void setDataCell(DataCell cell);
}