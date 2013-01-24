package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.splicemachine.derby.iapi.storage.RowProvider;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.hbase.SpliceOperationProtocol;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation.NodeType;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.utils.SpliceLogUtils;

public class OperationBranch {
	private static Logger LOG = Logger.getLogger(OperationBranch.class);
	protected List<SpliceOperation> spliceOperations;
	protected ExecRow rowDefinition;
	protected SpliceOperation regionOperation;
	protected SpliceOperation topOperation;
	protected RowProvider rowProvider;
	protected Activation activation;
	protected byte[] table;
	protected CountDownLatch countDownLatch;
	OperationBranch() {
		SpliceLogUtils.trace(LOG, "instantiated");
	}
	public OperationBranch(Activation activation, List<SpliceOperation> spliceOperations, ExecRow rowDefinition) {
		SpliceLogUtils.trace(LOG, "instantiated");
		this.spliceOperations = spliceOperations;
		this.rowDefinition = rowDefinition;
		this.activation = activation;
		init();
	}
	protected void init() {
		SpliceLogUtils.trace(LOG, "init");
		regionOperation = spliceOperations.get(0);
		topOperation = spliceOperations.get(spliceOperations.size()-1);
		if(regionOperation.getNodeTypes().contains(NodeType.REDUCE) && spliceOperations.size() > 1){
			rowProvider = regionOperation.getReduceRowProvider(regionOperation,rowDefinition);
			table = SpliceOperationCoprocessor.TEMP_TABLE;
		}else {
			rowProvider = regionOperation.getMapRowProvider(regionOperation,rowDefinition);
            table = rowProvider.getTableName();
		}
	}
	public SpliceOperation getRegionOperation() {
		SpliceLogUtils.trace(LOG, "getRegionOperation=%s",regionOperation);
		return regionOperation;
	}

	public SpliceOperation getTopOperation() {
		SpliceLogUtils.trace(LOG, "getTopOperation=%s",topOperation);
		return topOperation;
	}

	public SpliceOperation getRowProvider() {
		SpliceLogUtils.trace(LOG, "getTopOperation=%s",spliceOperations.get(spliceOperations.size()-1));
		return spliceOperations.get(spliceOperations.size()-1);
	}
	
	public void execCoprocessor() throws StandardException {
		SpliceLogUtils.trace(LOG, "execCoprocessor with top operation of %s", topOperation);
		final Scan scan = rowProvider.toScan();
        if(scan==null||table==null){
            SpliceLogUtils.debug(LOG,"Unable to exec coprocessor, no scan or table name present");
            return;
        }
        
		HTableInterface htable = null;
		try{
            SpliceLogUtils.debug(LOG,"Exec Coprocessor against table=%s",Bytes.toString(table));
			htable = SpliceAccessManager.getHTable(table);
			long numberCreated = 0;
			Map<byte[], Long> results = htable.coprocessorExec(SpliceOperationProtocol.class,scan.getStartRow(),scan.getStopRow(),new Batch.Call<SpliceOperationProtocol,Long>(){
				@Override
				public Long call(
						SpliceOperationProtocol instance)
								throws IOException {
					try{
						return instance.run((GenericStorablePreparedStatement)activation.getPreparedStatement(), 
																									scan, topOperation);
					}catch(StandardException se){
						SpliceLogUtils.logAndThrow(LOG, "Unexpected error executing coprocessor",new IOException(se));
						return -1l;
					}
				}
			});
			for(Long returnedRow : results.values()){
				numberCreated += returnedRow;
			}
			SpliceLogUtils.trace(LOG,"Completed %d shuffle tasks",numberCreated);
		}catch(IOException ioe){
			if(ioe.getCause() instanceof StandardException)
				SpliceLogUtils.logAndThrow(LOG, (StandardException)ioe.getCause());
			else
				SpliceLogUtils.logAndThrow(LOG,StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, ioe));
		}catch(Throwable t){
			SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,t));
		}finally{
			if(htable !=null ){
				try{
					htable.close();
				}catch(IOException e){
					SpliceLogUtils.logAndThrow(LOG,"Unable to release Hbase table back to pool",
							StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e));
				}
			}
		}
	}
}
