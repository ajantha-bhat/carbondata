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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, DataLoadTableFileMapping}
import org.apache.spark.sql.execution.command.management.CommonLoadUtils.transformQuery
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, HadoopFsRelation, LogicalRelation, SparkCarbonTableFormat}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.util.{CarbonReflectionUtils, CausedBy, FileUtils, SparkUtil}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.{CarbonCommonConstants, SortScopeOptions}
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.ICarbonLock
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util._
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.load.{CsvRDDHelper, DataLoadProcessorStepOnSpark}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.CarbonScalaUtil

case class CarbonLoadDataCommand(databaseNameOp: Option[String],
    tableName: String,
    factPathFromUser: String,
    dimFilesPath: Seq[DataLoadTableFileMapping],
    options: Map[String, String],
    isOverwriteTable: Boolean,
    var inputSqlString: String = null,
    partition: Map[String, Option[String]] = Map.empty,
    var operationContext: OperationContext = new OperationContext)
  extends AtomicRunnableCommand {

  var table: CarbonTable = _

  var logicalPartitionRelation: LogicalRelation = _

  var sizeInBytes: Long = _

  var currPartitions: util.List[PartitionSpec] = _

  var parentTablePath: String = _

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val (sizeInBytes, table, dbName, logicalPartitionRelation) =
      CommonLoadUtils.processMetadataCommon(sparkSession,
        databaseNameOp,
        tableName,
        None)
    this.sizeInBytes = sizeInBytes
    this.table = table
    this.logicalPartitionRelation = logicalPartitionRelation
    setAuditTable(dbName, tableName)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    var concurrentLoadLock: Option[ICarbonLock] = None
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    carbonProperty.addProperty("zookeeper.enable.lock", "false")
    val factPath = FileUtils.getPaths(factPathFromUser, hadoopConf)
    currPartitions = CommonLoadUtils.getCurrentParitions(sparkSession, table)
    CommonLoadUtils.setNumberOfCoresWhileLoading(sparkSession, carbonProperty)
    val optionsFinal: util.Map[String, String] =
      CommonLoadUtils.getFinalLoadOptions(
        carbonProperty, table, options)
    val carbonLoadModel: CarbonLoadModel = CommonLoadUtils.prepareLoadModel(
      hadoopConf,
      factPath,
      optionsFinal, parentTablePath, table, isDataFrame = false, Map.empty, partition, options)
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
      if (!table.isHivePartitionTable) {
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
      LOGGER.info("Sort Scope : " + carbonLoadModel.getSortScope)
      val loadParams = CarbonLoadParams(sparkSession,
        carbonProperty,
        carbonLoadModel,
        partitionStatus,
        hadoopConf,
        operationContext,
        isOverwriteTable,
        logicalPartitionRelation,
        partition,
        tableName,
        sizeInBytes,
        optionsFinal.asScala,
        currPartitions)
      val (rows, loadResult) = loadData(loadParams)
      val info = CommonLoadUtils.makeAuditInfo(loadResult)
      setAuditInfo(info)
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

  def loadData(loadParams: CarbonLoadParams): (Seq[Row], LoadMetadataDetails) = {
    var rows = Seq.empty[Row]
    val table = loadParams.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var loadResult : LoadMetadataDetails = null
    if (table.isHivePartitionTable) {
      rows = loadDataWithPartition(loadParams)
    } else {
      loadResult = CarbonDataRDDFactory.loadCarbonData(loadParams.sparkSession.sqlContext,
        loadParams.carbonLoadModel,
        loadParams.partitionStatus,
        isOverwriteTable,
        loadParams.hadoopConf,
        operationContext)
    }
    (rows, loadResult)
  }

  /**
   * Loads the data in a hive partition way. This method uses InsertIntoTable command to load data
   * into partitioned data. The table relation would be converted to HadoopFSRelation to let spark
   * handling the partitioning.
   */
  def loadDataWithPartition(loadParams: CarbonLoadParams): Seq[Row] = {
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
      val query: LogicalPlan = {
        val columnCount = loadParams.carbonLoadModel.getCsvHeaderColumns.length
        val rdd = CsvRDDHelper.csvFileScanRDD(
          loadParams.sparkSession,
          model = loadParams.carbonLoadModel,
          loadParams.hadoopConf).map(DataLoadProcessorStepOnSpark.toStringArrayRow(_, columnCount))
        val (transformedPlan, partitions, persistedRDDLocal) =
          CommonLoadUtils.transformQuery(
            rdd.asInstanceOf[RDD[Row]],
            loadParams.sparkSession,
            loadParams.carbonLoadModel,
            partitionValues,
            catalogTable,
            attributes,
            sortScope,
            isDataFrame = false, table, partition)
        partitionsLen = partitions
        persistedRDD = persistedRDDLocal
        transformedPlan
      }
      if (loadParams.carbonLoadModel.getFactTimeStamp == 0L) {
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
        operationContext, partition, loadParams.optionsOriginal, currPartitions)
      val convertedPlan =
        CarbonReflectionUtils.getInsertIntoCommand(
          table = convertRelation,
          partition = partition,
          query = query,
          overwrite = false,
          ifPartitionNotExists = false)
      SparkUtil.setNullExecutionId(loadParams.sparkSession)
      Dataset.ofRows(loadParams.sparkSession, convertedPlan)
    } catch {
      case ex: Throwable =>
        val (executorMessage, errorMessage) = CarbonScalaUtil.retrieveAndLogErrorMsg(ex, LOGGER)
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

  def convertToLogicalRelation(
      catalogTable: CatalogTable,
      sizeInBytes: Long,
      overWrite: Boolean,
      loadModel: CarbonLoadModel,
      sparkSession: SparkSession,
      operationContext: OperationContext,
      partition: Map[String, Option[String]],
      optionsOriginal: mutable.Map[String, String],
      currPartitions: util.List[PartitionSpec]): LogicalRelation = {
    val table = loadModel.getCarbonDataLoadSchema.getCarbonTable
    val metastoreSchema = StructType(catalogTable.schema.fields.map { f =>
      val column = table.getColumnByName(f.name)
      if (column.hasEncoding(Encoding.DICTIONARY)) {
        f.copy(dataType = IntegerType)
      } else if (f.dataType == TimestampType || f.dataType == DateType) {
        f.copy(dataType = LongType)
      } else {
        f
      }
    })
    val lazyPruningEnabled = sparkSession.sqlContext.conf.manageFilesourcePartitions
    val catalog = new CatalogFileIndex(
      sparkSession, catalogTable, sizeInBytes)
    if (!lazyPruningEnabled) {
      catalog.filterPartitions(Nil) // materialize all the partitions in memory
    }
    var partitionSchema =
      StructType(table.getPartitionInfo().getColumnSchemaList.asScala.map(field =>
        metastoreSchema.fields.find(_.name.equalsIgnoreCase(field.getColumnName))).map(_.get))
    val dataSchema =
      StructType(metastoreSchema
        .filterNot(field => partitionSchema.contains(field)))
    if (partition.nonEmpty) {
      partitionSchema = StructType(partitionSchema.fields.map(_.copy(dataType = StringType)))
    }
    val options = new mutable.HashMap[String, String]()
    options ++= catalogTable.storage.properties
    options += (("overwrite", overWrite.toString))
    if (partition.nonEmpty) {
      val staticPartitionStr = ObjectSerializationUtil.convertObjectToString(
        new util.HashMap[String, Boolean](
          partition.map { case (col, value) => (col.toLowerCase, value.isDefined) }.asJava))
      options += (("staticpartition", staticPartitionStr))
    }
    options ++= optionsOriginal
    if (currPartitions != null) {
      val currPartStr = ObjectSerializationUtil.convertObjectToString(currPartitions)
      options += (("currentpartition", currPartStr))
    }
    if (loadModel.getSegmentId != null) {
      val currLoadEntry =
        ObjectSerializationUtil.convertObjectToString(loadModel.getCurrentLoadMetadataDetail)
      options += (("currentloadentry", currLoadEntry))
    }
    val hdfsRelation = HadoopFsRelation(
      location = catalog,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      bucketSpec = catalogTable.bucketSpec,
      fileFormat = new SparkCarbonTableFormat,
      options = options.toMap)(sparkSession = sparkSession)

    CarbonReflectionUtils.getLogicalRelation(hdfsRelation,
      hdfsRelation.schema.toAttributes,
      Some(catalogTable),
      false)
  }


  override protected def opName: String = {
    if (isOverwriteTable) {
      "LOAD DATA OVERWRITE"
    } else {
      "LOAD DATA"
    }
  }
}
