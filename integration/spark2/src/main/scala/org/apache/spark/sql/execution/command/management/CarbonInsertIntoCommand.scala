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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, CarbonEnv, CarbonUtils, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.execution.command.{AtomicRunnableCommand, DataLoadTableFileMapping}
import org.apache.spark.sql.execution.command.management.CommonLoadUtils.convertToLogicalRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.{CarbonReflectionUtils, CausedBy, SparkUtil}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.{CarbonCommonConstants, SortScopeOptions}
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.indexserver.DistributedRDDUtils
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.CarbonScalaUtil

/*
* insert into without df, by just using logical plan
*
*/
case class CarbonInsertIntoCommand(databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String],
    isOverwriteTable: Boolean,
    var logicalPlan: LogicalPlan,
    var dimFilesPath: Seq[DataLoadTableFileMapping] = Seq(),
    var inputSqlString: String = null,
    var tableInfoOp: Option[TableInfo] = None,
    var internalOptions: Map[String, String] = Map.empty,
    var partition: Map[String, Option[String]] = Map.empty,
    var operationContext: OperationContext = new OperationContext)
  extends AtomicRunnableCommand {

  var table: CarbonTable = _

  var logicalPartitionRelation: LogicalRelation = _

  var sizeInBytes: Long = _

  var currPartitions: util.List[PartitionSpec] = _

  var parentTablePath: String = _

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  var scanResultRdd: RDD[InternalRow] = _

  var timeStampFormat: SimpleDateFormat = _

  var dateFormat: SimpleDateFormat = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    setAuditTable(CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession), tableName)
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    if (!tableInfoOp.isDefined) {
      throw new RuntimeException(" table info must be present when logical relation exist")
    }
    val (sizeInBytes, table, dbName, logicalPartitionRelation) = CommonLoadUtils
      .processMetadataCommon(
      sparkSession,
      databaseNameOp,
      tableName,
      tableInfoOp)
    this.sizeInBytes = sizeInBytes
    this.table = table
    this.logicalPartitionRelation = logicalPartitionRelation
    setAuditTable(dbName, tableName)
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
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

    val (tf, df) = CommonLoadUtils.getTimeAndDateFormatFromLoadModel(
      carbonLoadModel)
    timeStampFormat = tf
    dateFormat = df

    var complexChildCount: Int = 0
    var reArrangedIndex: Seq[Int] = Seq()
    var selectedColumnSchema: Seq[ColumnSchema] = Seq()
    var partitionIndex: Seq[Int] = Seq()

    val columnSchema = tableInfoOp.get.getFactTable.getListOfColumns.asScala
    val partitionInfo = tableInfoOp.get.getFactTable.getPartitionInfo
    val partitionColumnSchema =
      if (partitionInfo != null && partitionInfo.getColumnSchemaList.size() != 0) {
        partitionInfo.getColumnSchemaList.asScala
      } else {
        null
      }
    val convertedStaticPartition = mutable.Map[String, AnyRef]()
    // Remove the thread local entries of previous configurations.
    DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
    if (partition.nonEmpty) {
      for (col <- partitionColumnSchema) {
        if (partition(col.getColumnName.toLowerCase).isDefined) {
          convertedStaticPartition(col.getColumnName.toLowerCase) =
            CarbonScalaUtil.convertStaticPartitionToValues(partition(col.getColumnName.toLowerCase)
              .get,
              SparkDataTypeConverterImpl.convertCarbonToSparkDataType(col.getDataType),
              timeStampFormat,
              dateFormat)
        }
      }
    }
    val partitionColumnNames = if (partitionColumnSchema != null) {
      partitionColumnSchema.map(x => x.getColumnName).toSet
    } else {
      null
    }
    // get invisible column indexes, alter table scenarios can have it before or after new column
    // dummy measure will have ordinal -1 and it is invisible, ignore that column.
    // alter table old columns are just invisible columns with proper ordinal
    val invisibleIndex = columnSchema.filter(col => col.isInvisible && col.getSchemaOrdinal != -1)
      .map(col => col.getSchemaOrdinal)
    columnSchema.filterNot(col => col.isInvisible).foreach {
      col =>
        var skipPartitionColumn = false
        if (col.getColumnName.contains(".")) {
          // If the schema ordinal is -1,
          // no need to consider it during shifting columns to derive new shifted ordinal
          if (col.getSchemaOrdinal != -1) {
            complexChildCount = complexChildCount + 1
          }
        } else {
          // get number of invisible index count before this column
          val invisibleIndexCount = invisibleIndex.count(index => index < col.getSchemaOrdinal)
          if (col.getDataType.isComplexType) {
            // Calculate re-arrange index by ignoring the complex child count.
            // As projection will have only parent columns
            reArrangedIndex = reArrangedIndex :+
                              (col.getSchemaOrdinal - complexChildCount - invisibleIndexCount)
          } else {
            if (partitionColumnNames != null && partitionColumnNames.contains(col.getColumnName)) {
              partitionIndex = partitionIndex :+ (col.getSchemaOrdinal - invisibleIndexCount)
              skipPartitionColumn = true
            } else {
              reArrangedIndex = reArrangedIndex :+ (col.getSchemaOrdinal - invisibleIndexCount)
            }
          }
          if (!skipPartitionColumn) {
            selectedColumnSchema = selectedColumnSchema :+ col
          }
        }
    }
    if (partitionColumnSchema != null) {
      // keep partition columns in the end
      selectedColumnSchema = selectedColumnSchema ++ partitionColumnSchema
    }
    if (partitionIndex.nonEmpty) {
      // keep partition columns in the end and in the original create order
      reArrangedIndex = reArrangedIndex ++ partitionIndex.sortBy(x => x)
    }
    // If logical plan is unresolved, need to convert it to resolved.
    logicalPlan = Dataset.ofRows(sparkSession, logicalPlan).queryExecution.analyzed
    // Re-arrange the project as per columnSchema
    val newLogicalPlan = logicalPlan.transformDown {
      case p: Project =>
        var oldProjectionList = p.projectList
        if (partition.nonEmpty) {
          // partition keyword is present in insert and
          // select query partition projections may not be same as create order.
          // So, bring to create table order
          val dynamicPartition = partition.filterNot(entry => entry._2.isDefined)
          var index = 0
          val map = mutable.Map[String, Int]()
          for (part <- dynamicPartition) {
            map(part._1) = index
            index = index + 1
          }
          var tempList = oldProjectionList.take(oldProjectionList.size - dynamicPartition.size)
          val partitionList = oldProjectionList.takeRight(dynamicPartition.size)
          val partitionSchema = table.getPartitionInfo.getColumnSchemaList.asScala
          for (partitionCol <- partitionSchema) {
            if (map.get(partitionCol.getColumnName).isDefined) {
              tempList = tempList :+ partitionList(map(partitionCol.getColumnName))
            }
          }
          oldProjectionList = tempList
        }
        if (reArrangedIndex.size != oldProjectionList.size) {
          // for non-partition table columns must match
          if (partition.isEmpty) {
            throw new AnalysisException(
              s"Cannot insert into table $tableName because the number of columns are different: " +
              s"need ${ reArrangedIndex.size } columns, " +
              s"but query has ${ oldProjectionList.size } columns.")
          } else {
            if (reArrangedIndex.size - oldProjectionList.size != convertedStaticPartition.size) {
              throw new AnalysisException(
                s"Cannot insert into table $tableName because the number of columns are " +
                s"different: need ${ reArrangedIndex.size } columns, " +
                s"but query has ${ oldProjectionList.size } columns.")
            } else {
              // TODO: For partition case, remaining projections need to validate ?
            }
          }
        }
        var newProjectionList: Seq[NamedExpression] = Seq.empty
        var i = 0
        while (i < reArrangedIndex.size) {
          // column schema is already has sortColumns-dimensions-measures. Collect the ordinal &
          // re-arrange the projection in the same order
          if (partition.nonEmpty &&
              convertedStaticPartition.contains(selectedColumnSchema(i).getColumnName
                .toLowerCase())) {
            // If column schema present in partitionSchema means it is a static partition,
            // then add a value literal expression in the project.
            val value = convertedStaticPartition(selectedColumnSchema(i).getColumnName
              .toLowerCase())
            // TODO: use catalog table to get directly converted schema
            newProjectionList = newProjectionList :+
                                Alias(new Literal(value,
                                  SparkDataTypeConverterImpl.convertCarbonToSparkDataType(
                                    selectedColumnSchema(i).getDataType)), value.toString)(
                                  NamedExpression.newExprId,
                                  None,
                                  None).asInstanceOf[NamedExpression]
          } else {
            // If column schema NOT present in partition column,
            // get projection column mapping its ordinal.
            if (partition.contains(selectedColumnSchema(i).getColumnName.toLowerCase())) {
              // static partition + dynamic partition case,
              // here dynamic partition ordinal will be more than projection size
              newProjectionList = newProjectionList :+
                                  oldProjectionList(
                                    reArrangedIndex(i) - convertedStaticPartition.size)
            } else {
              newProjectionList = newProjectionList :+
                                  oldProjectionList(reArrangedIndex(i))
            }
          }
          i = i + 1
        }
        Project(newProjectionList, p.child)
    }
    scanResultRdd = sparkSession.sessionState.executePlan(newLogicalPlan).toRdd
    if (logicalPartitionRelation != null) {
      // re-arrange schema inside logical relation also
      if (reArrangedIndex.size != logicalPartitionRelation.schema.size) {
        throw new AnalysisException(
          s"Cannot insert into table $tableName because the number of columns are different: " +
          s"need ${ reArrangedIndex.size } columns, " +
          s"but query has ${ logicalPartitionRelation.schema.size } columns.")
      }
      val reArrangedFields = new Array[StructField](logicalPartitionRelation.schema.size)
      val reArrangedAttributes = new Array[AttributeReference](logicalPartitionRelation.schema.size)
      val fields = logicalPartitionRelation.schema.fields
      val output = logicalPartitionRelation.output
      var i = 0
      for (index <- reArrangedIndex) {
        reArrangedFields(i) = fields(index)
        reArrangedAttributes(i) = output(index)
        i = i + 1
      }
      val catalogTable = logicalPartitionRelation.catalogTable
        .get
        .copy(schema = new StructType(reArrangedFields))
      logicalPartitionRelation = logicalPartitionRelation.copy(logicalPartitionRelation.relation,
        reArrangedAttributes,
        Some(catalogTable))
    }
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

      val insertParams = CarbonInsertRddParams(sparkSession,
        carbonProperty,
        carbonLoadModel,
        partitionStatus,
        hadoopConf,
        operationContext,
        scanResultRdd,
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

  def insertData(loadParams: CarbonInsertRddParams): (Seq[Row], LoadMetadataDetails) = {
    var rows = Seq.empty[Row]
    val table = loadParams.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var loadResult : LoadMetadataDetails = null
    if (table.isHivePartitionTable) {
      rows = insertDataWithPartition(loadParams)
    } else {
      loadResult = CarbonDataRDDFactory.insertCarbonData(loadParams.sparkSession.sqlContext,
        loadParams.carbonLoadModel,
        loadParams.partitionStatus,
        isOverwriteTable,
        loadParams.hadoopConf,
        loadParams.scanResultRDD,
        operationContext)
    }
    (rows, loadResult)
  }

  /**
   * Loads the data in a hive partition way. This method uses InsertIntoTable command to load data
   * into partitioned data. The table relation would be converted to HadoopFSRelation to let spark
   * handling the partitioning.
   */
  def insertDataWithPartition(loadParams: CarbonInsertRddParams): Seq[Row] = {
    val table = loadParams.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val catalogTable: CatalogTable = logicalPartitionRelation.catalogTable.get
    // Clean up the alreday dropped partitioned data
    SegmentFileStore.cleanSegments(table, null, false)
    CarbonUtils.threadSet("partition.operationcontext", operationContext)
    // input data from csv files. Convert to logical plan
    val attributes = catalogTable.schema.toAttributes
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
      if (partition.nonEmpty) {
        val headers = loadParams.carbonLoadModel.getCsvHeaderColumns.dropRight(partition.size)
        val updatedHeader = headers ++ partition.keys.map(_.toLowerCase)
        loadParams.carbonLoadModel.setCsvHeader(updatedHeader.mkString(","))
        loadParams.carbonLoadModel
          .setCsvHeaderColumns(loadParams.carbonLoadModel.getCsvHeader.split(","))
      }
      // TODO: removed partition logic should be handle from re-arranging the projection itself.
      val (transformedPlan, partitions, persistedRDDLocal) =
        CommonLoadUtils.transformQueryWithInternalRow(
          loadParams.scanResultRDD,
          loadParams.sparkSession,
          loadParams.carbonLoadModel,
          partitionValues,
          catalogTable,
          attributes,
          sortScope,
          table,
          partition)
      partitionsLen = partitions
      persistedRDD = persistedRDDLocal
      if (loadParams.carbonLoadModel.getFactTimeStamp == 0L) {
        loadParams.carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
      }
      // Create and ddd the segment to the tablestatus.
      CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(loadParams.carbonLoadModel,
        isOverwriteTable)
      val opt = loadParams.optionsOriginal
      opt += (("no_rearrange_of_rows", "true"))
      val convertRelation = convertToLogicalRelation(
        catalogTable,
        sizeInBytes,
        isOverwriteTable,
        loadParams.carbonLoadModel,
        loadParams.sparkSession,
        operationContext, partition, None, opt, currPartitions)
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


  override protected def opName: String = {
    if (isOverwriteTable) {
      "INSERT OVERWRITE"
    } else {
      "INSERT INTO"
    }
  }
}
