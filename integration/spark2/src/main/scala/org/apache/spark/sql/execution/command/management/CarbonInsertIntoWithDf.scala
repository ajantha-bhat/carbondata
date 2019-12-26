/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.command.management

import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, CarbonUtils, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.{DataLoadTableFileMapping, UpdateTableModel}
import org.apache.spark.sql.execution.command.management.CommonLoadUtils.{convertToLogicalRelation, getLogicalQueryForUpdate, transformQuery}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{CarbonReflectionUtils, CausedBy, SparkUtil}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.{CarbonCommonConstants, SortScopeOptions}
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.CarbonScalaUtil

/*
* insert into with df, doesn't use logical plan
*
*/
case class CarbonInsertIntoWithDf(databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String],
    isOverwriteTable: Boolean,
    var dimFilesPath: Seq[DataLoadTableFileMapping] = Seq(),
    var dataFrame: DataFrame,
    var inputSqlString: String = null,
    var updateModel: Option[UpdateTableModel] = None,
    var tableInfoOp: Option[TableInfo] = None,
    var internalOptions: Map[String, String] = Map.empty,
    var partition: Map[String, Option[String]] = Map.empty,
    var operationContext: OperationContext = new OperationContext) {

  var table: CarbonTable = _

  var logicalPartitionRelation: LogicalRelation = _

  var sizeInBytes: Long = _

  var currPartitions: util.List[PartitionSpec] = _

  var parentTablePath: String = _

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def process(sparkSession: SparkSession): Seq[Row] = {
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    val (sizeInBytes, table, dbName, logicalPartitionRelation) = CommonLoadUtils
      .processMetadataCommon(
        sparkSession,
        databaseNameOp,
        tableName,
        tableInfoOp)
    this.sizeInBytes = sizeInBytes
    this.table = table
    this.logicalPartitionRelation = logicalPartitionRelation
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    carbonProperty.addProperty("zookeeper.enable.lock", "false")
    val factPath = ""
    currPartitions = CommonLoadUtils.getCurrentParitions(sparkSession, table)
    CommonLoadUtils.setNumberOfCoresWhileLoading(sparkSession, carbonProperty)
    val optionsFinal: util.Map[String, String] =
      CommonLoadUtils.getFinalLoadOptions(
      carbonProperty, table, options)
    val carbonLoadModel: CarbonLoadModel = CommonLoadUtils.prepareLoadModel(
      hadoopConf,
      factPath,
      optionsFinal, parentTablePath, table, isDataFrame = true, internalOptions, partition, options)
    // Delete stale segment folders that are not in table status but are physically present in
    // the Fact folder
    LOGGER.info(s"Deleting stale folders if present for table $dbName.$tableName")
    TableProcessingOperations.deletePartialLoadDataIfExist(table, false)
    var isUpdateTableStatusRequired = false
    val uuid = ""
    try {
      val (tableDataMaps, dataMapOperationContext) =
        CommonLoadUtils.firePreLoadEvents(sparkSession,
          carbonLoadModel,
          uuid,
          table,
          isOverwriteTable,
          operationContext)
      // First system has to partition the data first and then call the load data
      LOGGER.info(s"Initiating Direct Load for the Table : ($dbName.$tableName)")
      // Clean up the old invalid segment data before creating a new entry for new load.
      SegmentStatusManager.deleteLoadsAndUpdateMetadata(table, false, currPartitions)
      // add the start entry for the new load in the table status file
      if (updateModel.isEmpty && !table.isHivePartitionTable) {
        CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(
          carbonLoadModel,
          isOverwriteTable)
        isUpdateTableStatusRequired = true
      }
      if (isOverwriteTable) {
        LOGGER.info(s"Overwrite of carbon table with $dbName.$tableName is in progress")
      }
      // Create table and metadata folders if not exist
      if (carbonLoadModel.isCarbonTransactionalTable) {
        val metadataDirectoryPath = CarbonTablePath.getMetadataPath(table.getTablePath)
        if (!FileFactory.isFileExist(metadataDirectoryPath)) {
          FileFactory.mkdirs(metadataDirectoryPath)
        }
      } else {
        carbonLoadModel.setSegmentId(System.currentTimeMillis().toString)
      }
      val partitionStatus = SegmentStatus.SUCCESS

      val insertParams = CarbonInsertParams(sparkSession,
        carbonProperty,
        carbonLoadModel,
        partitionStatus,
        hadoopConf,
        operationContext,
        updateModel,
        dataFrame,
        isOverwriteTable,
        logicalPartitionRelation,
        partition,
        tableName,
        sizeInBytes,
        optionsFinal.asScala,
        currPartitions)

      LOGGER.info("Sort Scope : " + carbonLoadModel.getSortScope)
      val (rows, loadResult) = insertData(insertParams)
      val info = CommonLoadUtils.makeAuditInfo(loadResult)
      CommonLoadUtils.fireLoadPostEvent(sparkSession,
        carbonLoadModel,
        tableDataMaps,
        dataMapOperationContext,
        table,
        operationContext)
    } catch {
      case CausedBy(ex: NoRetryException) =>
        // update the load entry in table status file for changing the status to marked for delete
        if (isUpdateTableStatusRequired) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
        }
        LOGGER.error(s"Dataload failure for $dbName.$tableName", ex)
        throw new RuntimeException(s"Dataload failure for $dbName.$tableName, ${ex.getMessage}")
      // In case of event related exception
      case preEventEx: PreEventException =>
        LOGGER.error(s"Dataload failure for $dbName.$tableName", preEventEx)
        throw new AnalysisException(preEventEx.getMessage)
      case ex: Exception =>
        LOGGER.error(ex)
        // update the load entry in table status file for changing the status to marked for delete
        if (isUpdateTableStatusRequired) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
        }
        throw ex
    }
    Seq.empty
  }

  def insertData(loadParams: CarbonInsertParams): (Seq[Row], LoadMetadataDetails) = {
    var rows = Seq.empty[Row]
    val loadDataFrame = if (updateModel.isDefined) {
      val dataFrameWithTupleId: DataFrame = CommonLoadUtils.getDataFrameWithTupleID(Some(dataFrame))
      Some(dataFrameWithTupleId)
    } else {
      Some(dataFrame)
    }
    val table = loadParams.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var loadResult : LoadMetadataDetails = null
    loadParams.dataFrame = loadDataFrame.get
    if (table.isHivePartitionTable) {
      rows = insertDataWithPartition(loadParams)
    } else {
      loadResult = CarbonDataRDDFactory.insertCarbonDataWithDf(loadParams.sparkSession.sqlContext,
        loadParams.carbonLoadModel,
        loadParams.partitionStatus,
        isOverwriteTable,
        loadParams.hadoopConf,
        loadDataFrame,
        updateModel,
        operationContext)
    }
    (rows, loadResult)
  }

  /**
   * Loads the data in a hive partition way. This method uses InsertIntoTable command to load data
   * into partitioned data. The table relation would be converted to HadoopFSRelation to let spark
   * handling the partitioning.
   */
  def insertDataWithPartition(loadParams: CarbonInsertParams): Seq[Row] = {
    val table = loadParams.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val catalogTable: CatalogTable = logicalPartitionRelation.catalogTable.get
    var timeStampformatString = loadParams.carbonLoadModel.getTimestampformat
    if (timeStampformatString.isEmpty) {
      timeStampformatString = loadParams.carbonLoadModel.getDefaultTimestampFormat
    }
    val timeStampFormat = new SimpleDateFormat(timeStampformatString)
    var dateFormatString = loadParams.carbonLoadModel.getDateFormat
    if (dateFormatString.isEmpty) {
      dateFormatString = loadParams.carbonLoadModel.getDefaultDateFormat
    }
    val dateFormat = new SimpleDateFormat(dateFormatString)
    // Clean up the alreday dropped partitioned data
    SegmentFileStore.cleanSegments(table, null, false)
    CarbonUtils.threadSet("partition.operationcontext", operationContext)
    // input data from csv files. Convert to logical plan
    val allCols = new ArrayBuffer[String]()
    // get only the visible dimensions from table
    allCols ++= table.getVisibleDimensions().asScala.map(_.getColName)
    allCols ++= table.getVisibleMeasures.asScala.map(_.getColName)
    val attributes =
      StructType(
        allCols.filterNot(_.equals(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)).map(
          StructField(_, StringType))).toAttributes

    var partitionsLen = 0
    val sortScope = CarbonDataProcessorUtil.getSortScope(loadParams.carbonLoadModel.getSortScope)
    val partitionValues = if (partition.nonEmpty) {
      partition.filter(_._2.nonEmpty).map { case (col, value) =>
        catalogTable.schema.find(_.name.equalsIgnoreCase(col)) match {
          case Some(c) =>
            CarbonScalaUtil.convertToDateAndTimeFormats(
              value.get,
              c.dataType,
              timeStampFormat,
              dateFormat)
          case None =>
            throw new AnalysisException(s"$col is not a valid partition column in table ${
              loadParams.carbonLoadModel
                .getDatabaseName
            }.${ loadParams.carbonLoadModel.getTableName }")
        }
      }.toArray
    } else {
      Array[String]()
    }
    var persistedRDD: Option[RDD[InternalRow]] = None
    try {
      val (rdd, dfAttributes) = if (updateModel.isDefined) {
        // Get the updated query plan in case of update scenario
        val updatedFrame = Dataset.ofRows(
          loadParams.sparkSession,
          getLogicalQueryForUpdate(
            loadParams.sparkSession,
            catalogTable,
            loadParams.dataFrame,
            loadParams.carbonLoadModel))
        (updatedFrame.rdd, updatedFrame.schema)
      } else {
        if (partition.nonEmpty) {
          val headers = loadParams.carbonLoadModel.getCsvHeaderColumns.dropRight(partition.size)
          val updatedHeader = headers ++ partition.keys.map(_.toLowerCase)
          loadParams.carbonLoadModel.setCsvHeader(updatedHeader.mkString(","))
          loadParams.carbonLoadModel
            .setCsvHeaderColumns(loadParams.carbonLoadModel.getCsvHeader.split(","))
        }
        (loadParams.dataFrame.rdd, loadParams.dataFrame.schema)
      }

      val expectedColumns = {
        val staticPartCols = partition.filter(_._2.isDefined).keySet
        attributes.filterNot(a => staticPartCols.contains(a.name))
      }
      if (expectedColumns.length != dfAttributes.length) {
        throw new AnalysisException(
          s"Cannot insert into table $tableName because the number of columns are different: " +
          s"need ${ expectedColumns.length } columns, " +
          s"but query has ${ dfAttributes.length } columns.")
      }
      val nonPartitionBounds = expectedColumns.zipWithIndex.map(_._2).toArray
      val partitionBounds = new Array[Int](partitionValues.length)
      if (partition.nonEmpty) {
        val nonPartitionSchemaLen = attributes.length - partition.size
        var i = nonPartitionSchemaLen
        var index = 0
        var partIndex = 0
        partition.values.foreach { p =>
          if (p.isDefined) {
            partitionBounds(partIndex) = nonPartitionSchemaLen + index
            partIndex = partIndex + 1
          } else {
            nonPartitionBounds(i) = nonPartitionSchemaLen + index
            i = i + 1
          }
          index = index + 1
        }
      }

      val len = dfAttributes.length + partitionValues.length
      val transRdd = rdd.map { f =>
        val data = new Array[Any](len)
        var i = 0
        val length = f.length
        while (i < length) {
          data(nonPartitionBounds(i)) = f.get(i)
          i = i + 1
        }
        var j = 0
        val boundLength = partitionBounds.length
        while (j < boundLength) {
          data(partitionBounds(j)) = UTF8String.fromString(partitionValues(j))
          j = j + 1
        }
        Row.fromSeq(data)
      }

      val (transformedPlan, partitions, persistedRDDLocal) =
        transformQuery(
          transRdd,
          loadParams.sparkSession,
          loadParams.carbonLoadModel,
          partitionValues,
          catalogTable,
          attributes,
          sortScope,
          isDataFrame = true, table, partition)
      partitionsLen = partitions
      persistedRDD = persistedRDDLocal
      if (updateModel.isDefined) {
        loadParams.carbonLoadModel.setFactTimeStamp(updateModel.get.updatedTimeStamp)
      } else if (loadParams.carbonLoadModel.getFactTimeStamp == 0L) {
        loadParams.carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
      }
      // Create and ddd the segment to the tablestatus.
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadParams.carbonLoadModel,
        isOverwriteTable)
      val convertRelation = convertToLogicalRelation(
        catalogTable,
        sizeInBytes,
        isOverwriteTable,
        loadParams.carbonLoadModel,
        loadParams.sparkSession,
        operationContext, partition, updateModel, loadParams.optionsOriginal, currPartitions)
      val logicalPlan = if (sortScope == SortScopeOptions.SortScope.GLOBAL_SORT) {
        var numPartitions =
          CarbonDataProcessorUtil.getGlobalSortPartitions(loadParams
            .carbonLoadModel
            .getGlobalSortPartitions)
        if (numPartitions <= 0) {
          numPartitions = partitionsLen
        }
        if (numPartitions > 0) {
          Dataset
            .ofRows(loadParams.sparkSession, transformedPlan)
            .repartition(numPartitions)
            .logicalPlan
        } else {
          transformedPlan
        }
      } else {
        transformedPlan
      }

      val convertedPlan =
        CarbonReflectionUtils.getInsertIntoCommand(
          table = convertRelation,
          partition = partition,
          query = logicalPlan,
          overwrite = false,
          ifPartitionNotExists = false)
      SparkUtil.setNullExecutionId(loadParams.sparkSession)
      Dataset.ofRows(loadParams.sparkSession, convertedPlan)
    } catch {
      case ex: Throwable =>
        val (executorMessage, errorMessage) = CarbonScalaUtil.retrieveAndLogErrorMsg(ex, LOGGER)
        if (updateModel.isDefined) {
          CarbonScalaUtil.updateErrorInUpdateModel(updateModel.get, executorMessage)
        }
        LOGGER.info(errorMessage)
        LOGGER.error(ex)
        throw ex
    } finally {
      CarbonUtils.threadUnset("partition.operationcontext")
      if (isOverwriteTable) {
        DataMapStoreManager.getInstance().clearDataMaps(table.getAbsoluteTableIdentifier)
        // Clean the overwriting segments if any.
        SegmentFileStore.cleanSegments(
          table,
          null,
          false)
      }
      if (partitionsLen > 1) {
        // clean cache only if persisted and keeping unpersist non-blocking as non-blocking call
        // will not have any functional impact as spark automatically monitors the cache usage on
        // each node and drops out old data partitions in a least-recently used (LRU) fashion.
        persistedRDD match {
          case Some(rdd) => rdd.unpersist(false)
          case _ =>
        }
      }

      // Prepriming for Partition table here
      if (!StringUtils.isEmpty(loadParams.carbonLoadModel.getSegmentId)) {
        DistributedRDDUtils.triggerPrepriming(loadParams.sparkSession,
          table,
          Seq(),
          operationContext,
          loadParams.hadoopConf,
          List(loadParams.carbonLoadModel.getSegmentId))
      }
    }
    try {
      val compactedSegments = new util.ArrayList[String]()
      // Trigger auto compaction
      CarbonDataRDDFactory.handleSegmentMerging(
        loadParams.sparkSession.sqlContext,
        loadParams.carbonLoadModel
          .getCopyWithPartition(loadParams.carbonLoadModel.getCsvHeader,
            loadParams.carbonLoadModel.getCsvDelimiter),
        table,
        compactedSegments,
        operationContext)
      loadParams.carbonLoadModel.setMergedSegmentIds(compactedSegments)
    } catch {
      case e: Exception =>
        LOGGER.error(
          "Auto-Compaction has failed. Ignoring this exception because the " +
          "load is passed.", e)
    }
    val specs =
      SegmentFileStore.getPartitionSpecs(loadParams.carbonLoadModel.getSegmentId,
        loadParams.carbonLoadModel.getTablePath,
        SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(table.getTablePath)))
    if (specs != null) {
      specs.asScala.map { spec =>
        Row(spec.getPartitions.asScala.mkString("/"), spec.getLocation.toString, spec.getUuid)
      }
    } else {
      Seq.empty[Row]
    }
  }
}
