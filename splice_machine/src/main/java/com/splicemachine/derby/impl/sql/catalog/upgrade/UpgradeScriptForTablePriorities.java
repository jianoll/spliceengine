package com.splicemachine.derby.impl.sql.catalog.upgrade;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.impl.sql.catalog.SpliceDataDictionary;
import com.splicemachine.utils.SpliceLogUtils;

public class UpgradeScriptForTablePriorities extends UpgradeScriptBase {

    public UpgradeScriptForTablePriorities(SpliceDataDictionary sdd, TransactionController tc) {
        super(sdd, tc);
    }

    @Override
    protected void upgradeSystemTables() throws StandardException {
        // Exception handling here is WIP obviously
        try { sdd.upgradeTablePriorities(tc); }
        catch( Exception e) {}
    }
}
