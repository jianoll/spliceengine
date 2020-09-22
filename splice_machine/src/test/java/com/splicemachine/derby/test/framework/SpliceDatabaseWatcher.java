/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.test.framework;

import com.splicemachine.test_dao.SchemaDAO;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Semaphore;

public class SpliceDatabaseWatcher extends TestWatcher {
    public String dbName;

    public SpliceDatabaseWatcher(String dbName) {
        this.dbName = dbName;
    }

    @Override
    protected void starting(Description description) {
        try (Connection ignored = SpliceNetConnection.newBuilder().database(dbName).create(true).build()) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        super.starting(description);
    }

    @Override
    protected void finished(Description description) {
        // XXX (arnaud multidb implement finished for SpliceDatabaseWatcher)
    }

    @Override
    public String toString() {
        return dbName;
    }
}
