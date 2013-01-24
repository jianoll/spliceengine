package com.splicemachine.derby.impl.store.access;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.log4j.Logger;

public class ZookeeperTransactionContext extends ContextImpl {
	private static Logger LOG = Logger.getLogger(ZookeeperTransactionContext.class);
	private		ZookeeperTransaction	transaction;
	private     HBaseStore factory;
	private		boolean   abortAll; // true if any exception causes this transaction to be aborted.

	ZookeeperTransactionContext(ContextManager cm, String name, ZookeeperTransaction transaction, boolean abortAll, HBaseStore factory) {
		super(cm, name);

		this.transaction = transaction;
		this.abortAll = abortAll;
		this.factory = factory;
		transaction.transContext = this;	// double link between transaction and myself
	}


	/*
	** Context methods (most are implemented by super-class)
	*/


	/**
		@exception StandardException Standard Derby error policy
	*/
	public void cleanupOnError(Throwable error) throws StandardException {
		boolean throwAway = false;

		if (error instanceof StandardException) {
			StandardException se = (StandardException) error;

			if (abortAll) {
				// any error aborts an internal/nested xact and its transaction

				if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
					throw StandardException.newException(
                        SQLState.XACT_INTERNAL_TRANSACTION_EXCEPTION, error);
                }

				throwAway = true;


			} else {

				// If the severity is lower than a transaction error then do nothing.
				if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
					return;
                }
                 

				// If the session is going to disappear then we want to close this
				// transaction, not just abort it.
				if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY)
					throwAway = true;
			}
		} else {
			// some java* error, throw away the transaction.
			throwAway = true;
		}

		try {

			if (transaction != null) {
				// abort the transaction
				transaction.abort();
			}

		} catch (StandardException se) {
			// if we get an error during abort then shut the system down
			throwAway = true;

			// if the system was being shut down anyway, do nothing
			if ((se.getSeverity() <= ExceptionSeverity.SESSION_SEVERITY) &&
				(se.getSeverity() >= ((StandardException) error).getSeverity())) {

				throw StandardException.newException(SQLState.XACT_ABORT_EXCEPTION, se);
			}

		} finally {

			if (throwAway) {
				// xact close will pop this context out of the context
				// stack 
				transaction.close();
				transaction = null;
			}
		}

	}

	ZookeeperTransaction getTransaction() {
		return transaction;
	}

	HBaseStore getFactory() {
		return factory;
	}

	void substituteTransaction(ZookeeperTransaction newTran)
	{
		LOG.debug("substituteTransaction the old trans=(context="+transaction.getContextId()+",transName="+transaction.getTransactionName()
				+",status="+transaction.getTransactionStatus()+",id="+transaction.toString()+")");
		
		LOG.debug("substituteTransaction the old trans=(context="+newTran.getContextId()+",transName="+newTran.getTransactionName()
				+",status="+newTran.getTransactionStatus()+",id="+newTran.toString()+")");
		
		// disengage old tran from this xact context
		ZookeeperTransaction oldTran = (ZookeeperTransaction)transaction;
		if (oldTran.transContext == this)
			oldTran.transContext = null;

		// set up double link between new transaction and myself
		transaction = newTran;
		((ZookeeperTransaction)transaction).transContext = this;
	}

}
