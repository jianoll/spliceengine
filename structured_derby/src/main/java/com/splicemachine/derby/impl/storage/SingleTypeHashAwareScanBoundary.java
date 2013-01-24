package com.splicemachine.derby.impl.storage;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.sql.execute.operations.Hasher;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class SingleTypeHashAwareScanBoundary extends BaseHashAwareScanBoundary {
    private static final Logger LOG = Logger.getLogger(SingleTypeHashAwareScanBoundary.class);
	protected byte[] instructions;
    protected ExecRow execRow;
    protected Hasher hasher;

	public SingleTypeHashAwareScanBoundary(byte[] columnFamily, ExecRow execRow, Hasher hasher) {
		super(columnFamily);
		this.hasher = hasher;
		this.execRow = execRow;
	}
	
    @Override
    public byte[] getStartKey(Result result) {
        try {
            SpliceUtils.populate(result, execRow.getRowArray());
            return hasher.generateSortedHashScanKey(execRow.getRowArray());
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
        return null;
    }

    @Override
    public byte[] getStopKey(Result result) {
        try {
        	SpliceUtils.populate(result, execRow.getRowArray());
            byte[] start = hasher.generateSortedHashScanKey(execRow.getRowArray());
            BytesUtil.incrementAtIndex(start, start.length - 1);
            return start;
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }
        return null;
    }
	
}
