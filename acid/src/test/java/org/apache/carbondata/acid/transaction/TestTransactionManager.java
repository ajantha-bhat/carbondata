package org.apache.carbondata.acid.transaction;


public class TestTransactionManager {

  public static void main(String[] args) {
    TransactionDetail detail = CarbonTransactionManager.getInstance()
        .registerTransaction(false, TransactionOperation.LOAD, "table1");

    System.out.println("Transaction id is :" + detail.getTransactionId());

    CarbonTransactionManager.getInstance().commitTransaction(detail);

    TransactionDetail detail1 = CarbonTransactionManager.getInstance()
        .registerTransaction(false, TransactionOperation.UPDATE, "table1");
    System.out.println("Transaction id is :" + detail1.getTransactionId());

    CarbonTransactionManager.getInstance().commitTransaction(detail1);

    TransactionDetail detail2 = CarbonTransactionManager.getInstance()
        .registerTransaction(false, TransactionOperation.DELETE, "table1");
    System.out.println("Transaction id is :" + detail2.getTransactionId());

    CarbonTransactionManager.getInstance().commitTransaction(detail2);
  }

}
