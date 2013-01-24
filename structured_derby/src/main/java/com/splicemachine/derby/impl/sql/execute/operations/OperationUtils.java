
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation.NodeType;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class OperationUtils {

	private OperationUtils(){}
	
	public static void generateLeftOperationStack(SpliceOperation op,List<SpliceOperation> opAccumulator){
		SpliceOperation leftOp = op.getLeftOperation();
		if(leftOp !=null && !leftOp.getNodeTypes().contains(NodeType.REDUCE)){
			//recursively generateLeftOperationStack
			generateLeftOperationStack(leftOp,opAccumulator);
		}else if(leftOp!=null)
			opAccumulator.add(leftOp);
		opAccumulator.add(op);
	}
	
	public static List<SpliceOperation> getOperationStack(SpliceOperation op){
		List<SpliceOperation> ops = new LinkedList<SpliceOperation>();
		generateLeftOperationStack(op,ops);
		return ops;
	}

	public static NoPutResultSet executeScan(SpliceOperation operation,Logger log) {
		SpliceLogUtils.trace(log,"executeScan");
		final List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
		operation.generateLeftOperationStack(operationStack);
		SpliceLogUtils.trace(log, "operationStack=%s",operationStack);
		SpliceOperation regionOperation = operationStack.get(0);
		log.trace("regionOperation="+regionOperation);
		RowProvider provider;
		if (regionOperation.getNodeTypes().contains(NodeType.REDUCE) && operation != regionOperation) {
			SpliceLogUtils.trace(log,"scanning Temp Table");
			provider = regionOperation.getReduceRowProvider(operation,operation.getExecRowDefinition());
		} else {
			SpliceLogUtils.trace(log,"scanning Map Table");
			provider = regionOperation.getMapRowProvider(operation,operation.getExecRowDefinition());
		}
		return new SpliceNoPutResultSet(operation.getActivation(),operation, provider);
	}
}
