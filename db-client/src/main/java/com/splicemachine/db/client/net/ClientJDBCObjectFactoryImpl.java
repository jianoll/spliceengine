/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.net;

import java.sql.SQLException;
import com.splicemachine.db.client.ClientPooledConnection;
import com.splicemachine.db.client.ClientXAConnection;
import com.splicemachine.db.client.am.CachingLogicalConnection;
import com.splicemachine.db.client.am.CallableStatement;
import com.splicemachine.db.client.am.ClientJDBCObjectFactory;
import com.splicemachine.db.client.am.LogicalConnection;
import com.splicemachine.db.client.am.ParameterMetaData;
import com.splicemachine.db.client.am.PreparedStatement;
import com.splicemachine.db.client.am.LogicalCallableStatement;
import com.splicemachine.db.client.am.LogicalPreparedStatement;
import com.splicemachine.db.client.am.LogWriter;
import com.splicemachine.db.client.am.Agent;
import com.splicemachine.db.client.am.Section;
import com.splicemachine.db.client.am.Statement;
import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.client.am.Cursor;
import com.splicemachine.db.client.am.stmtcache.JDBCStatementCache;
import com.splicemachine.db.client.am.stmtcache.StatementKey;
import com.splicemachine.db.jdbc.ClientBaseDataSource;
import com.splicemachine.db.jdbc.ClientXADataSource;
import com.splicemachine.db.client.am.ColumnMetaData;
import com.splicemachine.db.client.am.StatementCacheInteractor;

/**
 * Implements the the ClientJDBCObjectFactory interface and returns the classes
 * that implement the JDBC3.0/2.0 interfaces
 * For Eg. newCallableStatement would return
 * com.splicemachine.db.client.am.CallableStatement
 */

public class ClientJDBCObjectFactoryImpl implements ClientJDBCObjectFactory{
    /**
     * Returns an instance of com.splicemachine.db.client.ClientPooledConnection
     */
    public ClientPooledConnection newClientPooledConnection(ClientBaseDataSource ds,
            LogWriter logWriter,String user,
            String password) throws SQLException {
        return new ClientPooledConnection(ds,logWriter,user,password);
    }
    /**
     * Returns an instance of com.splicemachine.db.client.ClientPooledConnection
     */
    public ClientPooledConnection newClientPooledConnection(ClientBaseDataSource ds,
            LogWriter logWriter,String user,
            String password,int rmId) throws SQLException {
        return new ClientPooledConnection(ds,logWriter,user,password,rmId);
    }
    /**
     * Returns an instance of com.splicemachine.db.client.ClientXAConnection
     */
    public ClientXAConnection newClientXAConnection(ClientBaseDataSource ds,
        LogWriter logWriter,String user, String password) throws SQLException
    {
        return new ClientXAConnection((ClientXADataSource)ds,
            (NetLogWriter)logWriter,user,password);
    }
    /**
     * Returns an instance of com.splicemachine.db.client.am.CallableStatement.
     *
     * @param agent       The instance of NetAgent associated with this
     *                    CallableStatement object.
     * @param connection  The connection object associated with this
     *                    PreparedStatement Object.
     * @param sql         A String object that is the SQL statement to be sent 
     *                    to the database.
     * @param type        One of the ResultSet type constants
     * @param concurrency One of the ResultSet concurrency constants
     * @param holdability One of the ResultSet holdability constants
     * @param cpc         The PooledConnection object that will be used to 
     *                    notify the PooledConnection reference of the Error 
     *                    Occurred and the Close events.
     * @return a CallableStatement object
     * @throws SqlException
     */
    public CallableStatement newCallableStatement(Agent agent,
            com.splicemachine.db.client.am.Connection connection,
            String sql,int type,int concurrency,
            int holdability,ClientPooledConnection cpc) throws SqlException {
        return new CallableStatement(agent,connection,sql,type,
                concurrency,holdability,cpc);
    }
   
    /**
     * Returns an instance of com.splicemachine.db.client.am.LogicalConnection
     */
    public LogicalConnection newLogicalConnection(
                    com.splicemachine.db.client.am.Connection physicalConnection,
                    ClientPooledConnection pooledConnection)
        throws SqlException {
        return new LogicalConnection(physicalConnection, pooledConnection);
    }
    
   /**
    * Returns an instance of a {@code CachingLogicalConnection}, which
    * provides caching of prepared statements.
    *
    * @param physicalConnection the underlying physical connection
    * @param pooledConnection the pooled connection
    * @param stmtCache statement cache
    * @return A logical connection with statement caching capabilities.
    *
    * @throws SqlException if creation of the logical connection fails
    */
    public LogicalConnection newCachingLogicalConnection(
            com.splicemachine.db.client.am.Connection physicalConnection,
            ClientPooledConnection pooledConnection,
            JDBCStatementCache stmtCache) throws SqlException {
        return new CachingLogicalConnection(physicalConnection,
                                            pooledConnection,
                                            stmtCache);
    }

    /**
     * This method returns an instance of PreparedStatement
     * which implements java.sql.PreparedStatement. It has the
     * ClientPooledConnection as one of its parameters
     * this is used to raise the Statement Events when the prepared
     * statement is closed.
     *
     * @param agent The instance of NetAgent associated with this
     *              CallableStatement object.
     * @param connection The connection object associated with this
     *                   PreparedStatement Object.
     * @param sql        A String object that is the SQL statement to be sent
     *                   to the database.
     * @param section    Section
     * @param cpc The ClientPooledConnection wraps the underlying physical
     *            connection associated with this prepared statement.
     *            It is used to pass the Statement closed and the Statement
     *            error occurred events that occur back to the
     *            ClientPooledConnection.
     * @return a PreparedStatement object
     * @throws SqlException
     */
    public PreparedStatement newPreparedStatement(Agent agent,
            com.splicemachine.db.client.am.Connection connection,
            String sql,Section section,ClientPooledConnection cpc) 
            throws SqlException {
        return new PreparedStatement(agent,connection,sql,section,cpc);
    }
    
    /**
     *
     * This method returns an instance of PreparedStatement
     * which implements java.sql.PreparedStatement.
     * It has the ClientPooledConnection as one of its parameters
     * this is used to raise the Statement Events when the prepared
     * statement is closed.
     *
     * @param agent The instance of NetAgent associated with this
     *              CallableStatement object.
     * @param connection  The connection object associated with this
     *                    PreparedStatement Object.
     * @param sql         A String object that is the SQL statement
     *                    to be sent to the database.
     * @param type        One of the ResultSet type constants.
     * @param concurrency One of the ResultSet concurrency constants.
     * @param holdability One of the ResultSet holdability constants.
     * @param autoGeneratedKeys a flag indicating whether auto-generated
     *                          keys should be returned.
     * @param columnNames an array of column names indicating the columns that
     *                    should be returned from the inserted row or rows.
     * @param cpc The ClientPooledConnection wraps the underlying physical
     *            connection associated with this prepared statement
     *            it is used to pass the Statement closed and the Statement
     *            error occurred events that occur back to the
     *            ClientPooledConnection.
     * @return a PreparedStatement object
     * @throws SqlException
     *
     */
    public PreparedStatement newPreparedStatement(Agent agent,
            com.splicemachine.db.client.am.Connection connection,
            String sql,int type,int concurrency,int holdability,
            int autoGeneratedKeys,String [] columnNames,
            int[] columnIndexes,
            ClientPooledConnection cpc)
            throws SqlException {
        return new PreparedStatement(agent,connection,sql,type,concurrency,
                holdability,autoGeneratedKeys,columnNames, columnIndexes, cpc);
    }

    /**
     * Returns a new logcial prepared statement object.
     *
     * @param ps underlying physical prepared statement
     * @param stmtKey key for the underlying physical prepared statement
     * @param cacheInteractor the statement cache interactor
     * @return A logical prepared statement.
     */
    public LogicalPreparedStatement newLogicalPreparedStatement(
            java.sql.PreparedStatement ps,
            StatementKey stmtKey,
            StatementCacheInteractor cacheInteractor) {
        return new LogicalPreparedStatement(ps, stmtKey, cacheInteractor);
    }

    /**
     * Returns a new logical callable statement object.
     *
     * @param cs underlying physical callable statement
     * @param stmtKey key for the underlying physical callable statement
     * @param cacheInteractor the statement cache interactor
     * @return A logical callable statement.
     */
    public LogicalCallableStatement newLogicalCallableStatement(
            java.sql.CallableStatement cs,
            StatementKey stmtKey,
            StatementCacheInteractor cacheInteractor) {
        return new LogicalCallableStatement(cs, stmtKey, cacheInteractor);
    }

    /**
     * returns an instance of com.splicemachine.db.client.net.NetConnection
     */
    public com.splicemachine.db.client.am.Connection newNetConnection(
            com.splicemachine.db.client.am.LogWriter netLogWriter,
            String databaseName,java.util.Properties properties)
            throws SqlException {
        return (com.splicemachine.db.client.am.Connection)
        (new NetConnection((NetLogWriter)netLogWriter,databaseName,properties));
    }
    /**
     * returns an instance of com.splicemachine.db.client.net.NetConnection
     */
    public com.splicemachine.db.client.am.Connection newNetConnection(
            com.splicemachine.db.client.am.LogWriter netLogWriter,
            com.splicemachine.db.jdbc.ClientBaseDataSource clientDataSource,
            String user,String password) throws SqlException {
        return  (com.splicemachine.db.client.am.Connection)
        (new NetConnection((NetLogWriter)netLogWriter,clientDataSource
                ,user,password));
    }
    /**
     * returns an instance of com.splicemachine.db.client.net.NetConnection
     */
    public com.splicemachine.db.client.am.Connection newNetConnection(
            com.splicemachine.db.client.am.LogWriter netLogWriter,
            int driverManagerLoginTimeout,String serverName,
            int portNumber,String databaseName,
            java.util.Properties properties) throws SqlException {
        return (com.splicemachine.db.client.am.Connection)
        (new NetConnection((NetLogWriter)netLogWriter,driverManagerLoginTimeout,
                serverName,portNumber,databaseName,properties));
    }
    /**
     * returns an instance of com.splicemachine.db.client.net.NetConnection
     */
    public com.splicemachine.db.client.am.Connection newNetConnection(
            com.splicemachine.db.client.am.LogWriter netLogWriter,String user,
            String password,
            com.splicemachine.db.jdbc.ClientBaseDataSource dataSource,
            int rmId,boolean isXAConn) throws SqlException {
        return (com.splicemachine.db.client.am.Connection)
        (new NetConnection((NetLogWriter)netLogWriter,user,password,dataSource,rmId,
                isXAConn));
    }
    /**
     * returns an instance of com.splicemachine.db.client.net.NetConnection
     */
    public com.splicemachine.db.client.am.Connection newNetConnection(
            com.splicemachine.db.client.am.LogWriter netLogWriter,
            String ipaddr,int portNumber,
            com.splicemachine.db.jdbc.ClientBaseDataSource dataSource,
            boolean isXAConn) throws SqlException {
        return (com.splicemachine.db.client.am.Connection)
        new NetConnection((NetLogWriter)netLogWriter,ipaddr,portNumber,dataSource,
                isXAConn);
    }
    /**
     * Returns an instance of com.splicemachine.db.client.net.NetConnection.
     * @param netLogWriter Placeholder for NetLogWriter object associated
     *                     with this connection.
     * @param user         user id for this connection.
     * @param password     password for this connection.
     * @param dataSource   The DataSource object passed from the PooledConnection
     *                     object from which this constructor was called.
     * @param rmId         The Resource Manager ID for XA Connections
     * @param isXAConn     true if this is a XA connection
     * @param cpc          The ClientPooledConnection object from which this
     *                     NetConnection constructor was called. This is used to
     *                     pass StatementEvents back to the pooledConnection
     *                     object.
     * @return a com.splicemachine.db.client.am.Connection object
     * @throws             SqlException
     */
    public com.splicemachine.db.client.am.Connection newNetConnection(
            com.splicemachine.db.client.am.LogWriter netLogWriter,String user,
            String password,
            com.splicemachine.db.jdbc.ClientBaseDataSource dataSource,
            int rmId,boolean isXAConn,
            ClientPooledConnection cpc) throws SqlException {
        return (com.splicemachine.db.client.am.Connection)
        (new NetConnection((NetLogWriter)netLogWriter,user,password,dataSource,rmId,
                isXAConn,cpc));
    }
    /**
     * returns an instance of com.splicemachine.db.client.net.NetResultSet
     */
    public com.splicemachine.db.client.am.ResultSet newNetResultSet(Agent netAgent,
            com.splicemachine.db.client.am.MaterialStatement netStatement,
            Cursor cursor,
            int qryprctyp,int sqlcsrhld,int qryattscr,int qryattsns,
            int qryattset,long qryinsid,int actualResultSetType,
            int actualResultSetConcurrency,
            int actualResultSetHoldability) throws SqlException {
        return new NetResultSet40((NetAgent)netAgent,
                (NetStatement)netStatement,cursor,qryprctyp,sqlcsrhld,qryattscr,
                qryattsns,qryattset,qryinsid,actualResultSetType,
                actualResultSetConcurrency,actualResultSetHoldability);
    }
    /**
     * returns an instance of com.splicemachine.db.client.net.NetDatabaseMetaData
     */
    public com.splicemachine.db.client.am.DatabaseMetaData newNetDatabaseMetaData(Agent netAgent,
            com.splicemachine.db.client.am.Connection netConnection) {
        return new NetDatabaseMetaData((NetAgent)netAgent,
                (NetConnection)netConnection);
    }
    
    /**
     * This method provides an instance of Statement 
     * @param  agent      Agent
     * @param  connection Connection
     * @return a java.sql.Statement implementation 
     * @throws SqlException
     *
     */
     public Statement newStatement(Agent agent, com.splicemachine.db.client.am.Connection connection)
                                            throws SqlException {
         return new Statement(agent,connection);
     }
     
     /**
     * This method provides an instance of Statement 
     * @param  agent            Agent
     * @param  connection       Connection
     * @param  type             int
     * @param  concurrency      int
     * @param  holdability      int
     * @param autoGeneratedKeys int
     * @param columnNames       String[]
     * @param columnIndexes     int[]
     * @return a java.sql.Statement implementation 
     * @throws SqlException
     *
     */
     public Statement newStatement(Agent agent, 
                     com.splicemachine.db.client.am.Connection connection, int type,
                     int concurrency, int holdability,
                     int autoGeneratedKeys, String[] columnNames,
                     int[] columnIndexes) 
                     throws SqlException {
         return new Statement(agent,connection,type,concurrency,holdability,
                 autoGeneratedKeys,columnNames, columnIndexes);
     }
     
     /**
     * Returns an instanceof ColumnMetaData 
     *
     * @param logWriter LogWriter
     * @return a ColumnMetaData implementation
     *
     */
    public ColumnMetaData newColumnMetaData(LogWriter logWriter) {
        return new ColumnMetaData(logWriter);
    }

    /**
     * Returns an instanceof ColumnMetaData or ColumnMetaData40 depending 
     * on the jdk version under use
     *
     * @param logWriter  LogWriter
     * @param upperBound int
     * @return a ColumnMetaData implementation
     *
     */
    public ColumnMetaData newColumnMetaData(LogWriter logWriter, int upperBound) {
        return new ColumnMetaData(logWriter,upperBound);
    }
    
    /**
     * 
     * returns an instance of ParameterMetaData 
     *
     * @param columnMetaData ColumnMetaData
     * @return a ParameterMetaData implementation
     *
     */
    public ParameterMetaData newParameterMetaData(ColumnMetaData columnMetaData) {
        return new ParameterMetaData(columnMetaData);
    }
}
