package org.apache.carbondata.acid.transaction;

// interface for handling transactions
public interface TransactionManager {

  /**
   * 
   * @param isGlobalTransaction
   * @param operation
   * @param tableGroupId
   * @return
   */
  public TransactionDetail registerTransaction(boolean isGlobalTransaction,
      TransactionOperation operation, String tableGroupId);

  /**
   * 
   * @param transactionDetail
   */
  public void commitTransaction(TransactionDetail transactionDetail);
  

}
