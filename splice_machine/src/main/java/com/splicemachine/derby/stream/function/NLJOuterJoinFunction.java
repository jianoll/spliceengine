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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iterator.NestedLoopJoinIterator;

import java.util.Iterator;

/**
 * Created by jleach on 4/24/15.
 */
public class NLJOuterJoinFunction<Op extends SpliceOperation> extends NLJoinFunction<Op, Iterator<ExecRow>, ExecRow> {

    public NLJOuterJoinFunction() {}

    public NLJOuterJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> from) throws Exception {
        if (!initialized) {
            init(from);
            initialized = true;
        }

        return new NestedLoopJoinIterator<>(this);
    }

    protected void init(Iterator<ExecRow> from) throws StandardException {
        joinType = JoinType.LEFT_OUTER;
        super.init(from);
    }
}
