package org.apache.carbondata.acid.transaction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import static org.apache.carbondata.core.util.CarbonUtil.closeStreams;

public final class CarbonTransactionManager implements TransactionManager {

  private static final CarbonTransactionManager INSTANCE = new CarbonTransactionManager();

  private static final InheritableThreadLocal<TransactionDetail> threadLocal =
      new InheritableThreadLocal<>();

  private CarbonTransactionManager() { }

  public static CarbonTransactionManager getInstance() {
    return INSTANCE;
  }

  public TransactionDetail getActiveTransaction() {
    return threadLocal.get();
  }

  @Override
  public TransactionDetail registerTransaction(boolean isGlobalTransaction,
      TransactionOperation operation, String tableGroupId) {

    TransactionDetail detail =
        new TransactionDetail(UUID.randomUUID().toString(), isGlobalTransaction, operation,
            tableGroupId);

    threadLocal.set(detail);

    return detail;
  }

  @Override
  public void commitTransaction(TransactionDetail transactionDetail) {
    // TODO: write transaction log json file
    try {

      TransactionDetail[] transactionDetails = readTransactionLog(
          "/home/root1/ab/txn/" + transactionDetail.getTableGroupId() + "_transaction_log");
      List<TransactionDetail> details = new ArrayList<>();

      details.add(transactionDetail);
      details.addAll(Arrays.asList(transactionDetails));
      writeTransactionLog(details,
          "/home/root1/ab/txn/" + transactionDetail.getTableGroupId() + "_transaction_log");
    } catch (IOException e) {
      //      e.printStackTrace();
    }
    // TODO: update table status with the version id
    threadLocal.remove();
  }

  private static void writeTransactionLog(List<TransactionDetail> transactionDetail, String path) throws
      IOException {
    //TODO: add a lock similar to segment lock
    AtomicFileOperations fileWrite =
        AtomicFileOperationFactory.getAtomicFileOperations(path);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(transactionDetail);
      brWriter.write(metadataInstance);
      brWriter.flush();
    } catch (IOException ie) {
      fileWrite.setFailed();
      throw ie;
    } finally {
      closeStreams(brWriter);
      fileWrite.close();    }
  }

  private static TransactionDetail[] readTransactionLog(String filePath) {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    TransactionDetail[] transactionDetails;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(filePath);

    try {
      if (!FileFactory
          .isFileExist(filePath)) {
        return new TransactionDetail[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          CarbonCommonConstants.DEFAULT_CHARSET);
      buffReader = new BufferedReader(inStream);
      Type listType = new TypeToken<List<TransactionDetail>>() {}.getType();
      transactionDetails = gsonObjectToRead.fromJson(buffReader, listType);
    } catch (IOException e) {
      return new TransactionDetail[0];
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }
    return transactionDetails;
  }

}
