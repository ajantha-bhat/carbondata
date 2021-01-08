package org.apache.carbondata.acid.transaction;

import java.io.Serializable;

// holder for current transaction information
public class TransactionDetail implements Serializable {

  private String TransactionId;

  private boolean isGlobalTransaction;

  private String TimeStamp;

  private TransactionOperation operation;

  // in case of single table, this will be dbname.tableName
  // in case of multiple table, this will be dbname.tablename1,dbname.tablename2
  private String tableGroupId; // how to support upgrade scenario ?

  public TransactionDetail(String transactionId, Boolean isGlobalTransaction,
      TransactionOperation operation, String tableGroupId) {
    TransactionId = transactionId;
    this.isGlobalTransaction = isGlobalTransaction;
    this.operation = operation;
    this.tableGroupId = tableGroupId;
  }

  public String getTransactionId() {
    return TransactionId;
  }

  public boolean getGlobalTransaction() {
    return isGlobalTransaction;
  }

  public String getTimeStamp() {
    return TimeStamp;
  }

  public TransactionOperation getOperation() {
    return operation;
  }

  public String getTableGroupId() {
    return tableGroupId;
  }

  public void setTimeStamp(String timeStamp) {
    TimeStamp = timeStamp;
  }
}
