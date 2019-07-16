/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.Pair;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Created by jyuan on 10/10/16.
 */
public class GetNLJoinInnerIterator extends GetNLJoinIterator {

    public GetNLJoinInnerIterator() {}

    public GetNLJoinInnerIterator(Supplier<OperationContext> operationContext, SynchronousQueue<ExecRow> in, BlockingQueue<Pair<GetNLJoinIterator, Iterator<ExecRow>>> out) {
        super(operationContext, in, out);
    }

    @Override
    public void run() {
        try {
            while (true) {
                ExecRow locatedRow = null;
                operationContext = operationContextSupplier.get();
                while (locatedRow == null ) {
                    locatedRow = in.poll(1, TimeUnit.SECONDS);
                    if (closed || operationContext.getOperation().isClosed())
                        break;
                }
                
                JoinOperation op = (JoinOperation) operationContext.getOperation();
                op.getLeftOperation().setCurrentRow(locatedRow);
                SpliceOperation rightOperation = op.getRightOperation();

                rightOperation.openCore(EngineDriver.driver().processorFactory().localProcessor(op.getActivation(), op));
                Iterator<ExecRow> rightSideNLJIterator = rightOperation.getExecRowIterator();
                // Lets make sure we perform a call...
                boolean hasNext = rightSideNLJIterator.hasNext();

                out.put(new Pair<>(this, rightSideNLJIterator));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
