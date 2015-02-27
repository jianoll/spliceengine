package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ByteColumnStatistics;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.collector.ByteColumnStatsCollector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class TinyintStatsCollector extends DvdStatsCollector{
    private final ByteColumnStatsCollector baseCollector;

    public TinyintStatsCollector(ByteColumnStatsCollector collector) {
        super(collector);
        this.baseCollector= collector;
    }

    @Override
    protected void doUpdate(DataValueDescriptor dataValueDescriptor, long count) throws StandardException {
        baseCollector.update(dataValueDescriptor.getByte(),count);
    }

    @Override
    protected ColumnStatistics<DataValueDescriptor> newStats(ColumnStatistics build) {
        return new TinyintStats((ByteColumnStatistics)build);
    }

}
