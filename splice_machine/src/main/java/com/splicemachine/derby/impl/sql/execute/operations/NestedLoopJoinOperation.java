package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.NLJAntiJoinFunction;
import com.splicemachine.derby.stream.function.NLJInnerJoinFunction;
import com.splicemachine.derby.stream.function.NLJOneRowInnerJoinFunction;
import com.splicemachine.derby.stream.function.NLJOuterJoinFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class NestedLoopJoinOperation extends JoinOperation {
		private static Logger LOG = Logger.getLogger(NestedLoopJoinOperation.class);
		protected boolean isHash;
        protected static final String NAME = NestedLoopJoinOperation.class.getSimpleName().replaceAll("Operation","");
    	@Override
    	public String getName() {
    			return NAME;
    	}
		public NestedLoopJoinOperation() {
				super();
		}

		public NestedLoopJoinOperation(SpliceOperation leftResultSet,
																	 int leftNumCols,
																	 SpliceOperation rightResultSet,
																	 int rightNumCols,
																	 Activation activation,
																	 GeneratedMethod restriction,
																	 int resultSetNumber,
																	 boolean oneRowRightSide,
																	 boolean notExistsRightSide,
																	 double optimizerEstimatedRowCount,
																	 double optimizerEstimatedCost,
																	 String userSuppliedOptimizerOverrides) throws StandardException {
				super(leftResultSet,leftNumCols,rightResultSet,rightNumCols,activation,restriction,
								resultSetNumber,oneRowRightSide,notExistsRightSide,optimizerEstimatedRowCount,
								optimizerEstimatedCost,userSuppliedOptimizerOverrides);
				this.isHash = false;
        try {
            init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
				super.readExternal(in);
				isHash = in.readBoolean();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeBoolean(isHash);
		}

		@Override
		public void init(SpliceOperationContext context) throws IOException, StandardException{
				super.init(context);
		}

		@Override
		public String toString() {
				return "NestedLoop"+super.toString();
		}

		@Override
		public String prettyPrint(int indentLevel) {
				return "NestedLoopJoin:" + super.prettyPrint(indentLevel);
		}


    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        DataSet<LocatedRow> left = leftResultSet.getDataSet(dsp);
        OperationContext<NestedLoopJoinOperation> operationContext = dsp.createOperationContext(this);

        operationContext.pushScope();
        try {
            if (isOuterJoin)
                return left.flatMap(new NLJOuterJoinFunction(operationContext), true);
            else {
                if (notExistsRightSide)
                    return left.flatMap(new NLJAntiJoinFunction(operationContext), true);
                else {
                    if (oneRowRightSide)
                        return left.flatMap(new NLJOneRowInnerJoinFunction(operationContext), true);
                    else
                        return left.flatMap(new NLJInnerJoinFunction(operationContext), true);
                }
            }
        } finally {
            operationContext.popScope();
        }
    }
}
