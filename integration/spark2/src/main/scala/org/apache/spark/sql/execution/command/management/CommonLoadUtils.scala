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
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Sort}
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.command.UpdateTableModel
import org.apache.spark.sql.execution.datasources.{CatalogFileIndex, FindDataSourceTable, HadoopFsRelation, LogicalRelation, SparkCarbonTableFormat}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants, SortScopeOptions}
import org.apache.carbondata.core.datamap.{DataMapStoreManager, TableDataMap}
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.row.CarbonRow
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util._
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, BuildDataMapPreExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.ComplexDelimitersEnum
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTablePostExecutionEvent, LoadTablePreExecutionEvent}
import org.apache.carbondata.processing.loading.model.{CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.{CarbonBadRecordUtil, CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.load.{CsvRDDHelper, DataLoadProcessorStepOnSpark, GlobalSortHelper}
import org.apache.carbondata.spark.util.CarbonScalaUtil

object CommonLoadUtils {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def makeAuditInfo(loadResult: LoadMetadataDetails): Map[String, String] = {
    if (loadResult != null) {
      Map(
        "SegmentId" -> loadResult.getLoadName,
        "DataSize" -> Strings.formatSize(java.lang.Long.parseLong(loadResult.getDataSize)),
        "IndexSize" -> Strings.formatSize(java.lang.Long.parseLong(loadResult.getIndexSize)))
    } else {
      Map()
    }
  }

  def getDataFrameWithTupleID(dataFrame: Option[DataFrame]): DataFrame = {
    val fields = dataFrame.get.schema.fields
    import org.apache.spark.sql.functions.udf
    // extracting only segment from tupleId
    val getSegIdUDF = udf((tupleId: String) =>
      CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID))
    // getting all fields except tupleId field as it is not required in the value
    val otherFields = CarbonScalaUtil.getAllFieldsWithoutTupleIdField(fields)
    // extract tupleId field which will be used as a key
    val segIdColumn = getSegIdUDF(new Column(UnresolvedAttribute
      .quotedString(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))).
      as(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_SEGMENTID)
    val fieldWithTupleId = otherFields :+ segIdColumn
    // use dataFrameWithTupleId as loadDataFrame
    val dataFrameWithTupleId = dataFrame.get.select(fieldWithTupleId: _*)
    (dataFrameWithTupleId)
  }

  def processMetadataCommon(sparkSession: SparkSession,
      databaseNameOp: Option[String],
      tableName: String,
      tableInfoOp: Option[TableInfo]): (Long, CarbonTable, String, LogicalRelation) = {
    val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    var table: CarbonTable = null
    var logicalPartitionRelation: LogicalRelation = null
    var sizeInBytes: Long = 0L

    table = if (tableInfoOp.isDefined) {
      CarbonTable.buildFromTableInfo(tableInfoOp.get)
    } else {
      val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
      if (relation == null) {
        throw new NoSuchTableException(dbName, tableName)
      }
      if (null == relation.carbonTable) {
        LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
        throw new NoSuchTableException(dbName, tableName)
      }
      relation.carbonTable
    }
    if (table.isHivePartitionTable) {
      logicalPartitionRelation =
        new FindDataSourceTable(sparkSession).apply(
          sparkSession.sessionState.catalog.lookupRelation(
            TableIdentifier(tableName, databaseNameOp))).collect {
          case l: LogicalRelation => l
        }.head
      sizeInBytes = logicalPartitionRelation.relation.sizeInBytes
    }
    (sizeInBytes, table, dbName, logicalPartitionRelation)
  }

  def prepareLoadModel(hadoopConf: Configuration,
      factPath: String,
      optionsFinal: util.Map[String, String],
      parentTablePath: String,
      table: CarbonTable,
      isDataFrame: Boolean,
      internalOptions: Map[String, String],
      partition: Map[String, Option[String]],
      options: Map[java.lang.String, String]): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setParentTablePath(parentTablePath)
    carbonLoadModel.setFactFilePath(factPath)
    carbonLoadModel.setCarbonTransactionalTable(table.getTableInfo.isTransactionalTable)
    carbonLoadModel.setAggLoadRequest(
      internalOptions.getOrElse(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL,
        CarbonCommonConstants.IS_INTERNAL_LOAD_CALL_DEFAULT).toBoolean)
    carbonLoadModel.setSegmentId(internalOptions.getOrElse("mergedSegmentName", ""))
    val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.COMPRESSOR,
        CompressorFactory.getInstance().getCompressor.getName)
    carbonLoadModel.setColumnCompressor(columnCompressor)
    carbonLoadModel.setRangePartitionColumn(table.getRangeColumn)
    val javaPartition = mutable.Map[String, String]()
    partition.foreach { case (k, v) =>
      if (v.isEmpty) {
        javaPartition(k) = null
      } else {
        javaPartition(k) = v.get
      }
    }
    new CarbonLoadModelBuilder(table).build(
      options.asJava,
      optionsFinal,
      carbonLoadModel,
      hadoopConf,
      javaPartition.asJava,
      isDataFrame)
    carbonLoadModel
  }

  def setNumberOfCoresWhileLoading(sparkSession: SparkSession,
      carbonProperty: CarbonProperties): CarbonProperties = {
    // get the value of 'spark.executor.cores' from spark conf, default value is 1
    val sparkExecutorCores = sparkSession.sparkContext.conf.get("spark.executor.cores", "1")
    // get the value of 'carbon.number.of.cores.while.loading' from carbon properties,
    // default value is the value of 'spark.executor.cores'
    val numCoresLoading =
    try {
      CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.NUM_CORES_LOADING, sparkExecutorCores)
    } catch {
      case exc: NumberFormatException =>
        LOGGER.error("Configured value for property " + CarbonCommonConstants.NUM_CORES_LOADING
                     + " is wrong. Falling back to the default value "
                     + sparkExecutorCores)
        sparkExecutorCores
    }
    // update the property with new value
    carbonProperty.addProperty(CarbonCommonConstants.NUM_CORES_LOADING, numCoresLoading)
  }

  def getCurrentParitions(sparkSession: SparkSession,
      table: CarbonTable): util.List[PartitionSpec] = {
    val currPartitions = if (table.isHivePartitionTable) {
      CarbonFilters.getCurrentPartitions(
        sparkSession,
        table) match {
        case Some(parts) => new util.ArrayList(parts.toList.asJava)
        case _ => null
      }
    } else {
      null
    }
    currPartitions
  }

  def getFinalLoadOptions(carbonProperty: CarbonProperties,
      table: CarbonTable,
      options: Map[java.lang.String, String]): util.Map[String, String] = {
    val tableProperties = table.getTableInfo.getFactTable.getTableProperties
    val optionsFinal = LoadOption.fillOptionWithDefaultValue(options.asJava)
    /**
     * Priority of sort_scope assignment :
     * -----------------------------------
     *
     * 1. Load Options  ->
     * LOAD DATA INPATH 'data.csv' INTO TABLE tableName OPTIONS('sort_scope'='no_sort')
     *
     * 2. Session property CARBON_TABLE_LOAD_SORT_SCOPE  ->
     * SET CARBON.TABLE.LOAD.SORT.SCOPE.database.table=local_sort
     *
     * 3. Sort Scope provided in TBLPROPERTIES
     * 4. Session property CARBON_OPTIONS_SORT_SCOPE
     * 5. Default Sort Scope LOAD_SORT_SCOPE
     */
    if (table.getNumberOfSortColumns == 0) {
      // If tableProperties.SORT_COLUMNS is null
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        SortScopeOptions.SortScope.NO_SORT.name)
    } else if (StringUtils.isBlank(tableProperties.get(CarbonCommonConstants.SORT_SCOPE))) {
      // If tableProperties.SORT_COLUMNS is not null
      // and tableProperties.SORT_SCOPE is null
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        options.getOrElse(CarbonCommonConstants.SORT_SCOPE,
          carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE +
                                     table.getDatabaseName + "." + table.getTableName,
            carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
              carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
                SortScopeOptions.SortScope.LOCAL_SORT.name)))))
    } else {
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        options.getOrElse(CarbonCommonConstants.SORT_SCOPE,
          carbonProperty.getProperty(
            CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE + table.getDatabaseName + "." +
            table.getTableName,
            tableProperties.asScala.getOrElse(CarbonCommonConstants.SORT_SCOPE,
              carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
                carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
                  CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))))
      if (!StringUtils.isBlank(tableProperties.get("global_sort_partitions"))) {
        if (options.get("global_sort_partitions").isEmpty) {
          optionsFinal.put(
            "global_sort_partitions",
            tableProperties.get("global_sort_partitions"))
        }
      }

    }
    optionsFinal
      .put("bad_record_path", CarbonBadRecordUtil.getBadRecordsPath(options.asJava, table))
    optionsFinal
  }

  def firePreLoadEvents(sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      uuid: String,
      table: CarbonTable,
      isOverwriteTable: Boolean,
      operationContext: OperationContext): (util.List[TableDataMap], OperationContext) = {
    operationContext.setProperty("uuid", uuid)
    val loadTablePreExecutionEvent: LoadTablePreExecutionEvent =
      new LoadTablePreExecutionEvent(
        table.getCarbonTableIdentifier,
        carbonLoadModel)
    operationContext.setProperty("isOverwrite", isOverwriteTable)
    OperationListenerBus.getInstance.fireEvent(loadTablePreExecutionEvent, operationContext)
    // Add pre event listener for index datamap
    val tableDataMaps = DataMapStoreManager.getInstance().getAllDataMap(table)
    val dataMapOperationContext = new OperationContext()
    if (tableDataMaps.size() > 0) {
      val dataMapNames: mutable.Buffer[String] =
        tableDataMaps.asScala.map(dataMap => dataMap.getDataMapSchema.getDataMapName)
      val buildDataMapPreExecutionEvent: BuildDataMapPreExecutionEvent =
        BuildDataMapPreExecutionEvent(sparkSession,
          table.getAbsoluteTableIdentifier, dataMapNames)
      OperationListenerBus.getInstance().fireEvent(buildDataMapPreExecutionEvent,
        dataMapOperationContext)
    }
    (tableDataMaps, dataMapOperationContext)
  }

  def fireLoadPostEvent(sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      tableDataMaps: util.List[TableDataMap],
      dataMapOperationContext: OperationContext,
      table: CarbonTable,
      operationContext: OperationContext): Unit = {
    val loadTablePostExecutionEvent: LoadTablePostExecutionEvent =
      new LoadTablePostExecutionEvent(
        table.getCarbonTableIdentifier,
        carbonLoadModel)
    OperationListenerBus.getInstance.fireEvent(loadTablePostExecutionEvent, operationContext)
    if (tableDataMaps.size() > 0) {
      val buildDataMapPostExecutionEvent = BuildDataMapPostExecutionEvent(sparkSession,
        table.getAbsoluteTableIdentifier, null, Seq(carbonLoadModel.getSegmentId), false)
      OperationListenerBus.getInstance()
        .fireEvent(buildDataMapPostExecutionEvent, dataMapOperationContext)
    }
  }

  /**
   * Convert the rdd as per steps of data loading inputprocessor step and converter step
   * @param originRDD
   * @param sparkSession
   * @param model
   * @param isDataFrame
   * @param partitionValues
   * @return
   */
  private def convertData(
      originRDD: RDD[Row],
      sparkSession: SparkSession,
      model: CarbonLoadModel,
      isDataFrame: Boolean,
      partitionValues: Array[String]): RDD[InternalRow] = {
    val sc = sparkSession.sparkContext
    val info =
      model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo.getFactTable.getPartitionInfo
    info.setColumnSchemaList(new util.ArrayList[ColumnSchema](info.getColumnSchemaList))
    val modelBroadcast = sc.broadcast(model)
    val partialSuccessAccum = sc.accumulator(0, "Partial Success Accumulator")

    val inputStepRowCounter = sc.accumulator(0, "Input Processor Accumulator")
    // 1. Input
    val convertRDD =
      if (isDataFrame) {
        originRDD.mapPartitions{rows =>
          DataLoadProcessorStepOnSpark.toRDDIterator(rows, modelBroadcast)
        }
      } else {
        // Append the partition columns in case of static partition scenario
        val partitionLen = partitionValues.length
        val len = model.getCsvHeaderColumns.length - partitionLen
        originRDD.map{ row =>
          val array = new Array[AnyRef](len + partitionLen)
          var i = 0
          while (i < len) {
            array(i) = row.get(i).asInstanceOf[AnyRef]
            i = i + 1
          }
          if (partitionLen > 0) {
            System.arraycopy(partitionValues, 0, array, i, partitionLen)
          }
          array
        }
      }
    val conf = SparkSQLUtil
      .broadCastHadoopConf(sparkSession.sparkContext, sparkSession.sessionState.newHadoopConf())
    val finalRDD = convertRDD.mapPartitionsWithIndex { case(index, rows) =>
      DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
      ThreadLocalSessionInfo.setConfigurationToCurrentThread(conf.value.value)
      DataLoadProcessorStepOnSpark.inputAndconvertFunc(
        rows,
        index,
        modelBroadcast,
        partialSuccessAccum,
        inputStepRowCounter,
        keepActualData = true)
    }.filter(_ != null).map(row => InternalRow.fromSeq(row.getData))

    finalRDD
  }

  /**
   * Transform the rdd to logical plan as per the sortscope. If it is global sort scope then it
   * will convert to sort logical plan otherwise project plan.
   */
  def transformQuery(rdd: RDD[Row],
      sparkSession: SparkSession,
      loadModel: CarbonLoadModel,
      partitionValues: Array[String],
      catalogTable: CatalogTable,
      curAttributes: Seq[AttributeReference],
      sortScope: SortScopeOptions.SortScope,
      isDataFrame: Boolean,
      table: CarbonTable,
      partition: Map[String, Option[String]]): (LogicalPlan, Int, Option[RDD[InternalRow]]) = {
    val catalogAttributes = catalogTable.schema.toAttributes
    // Converts the data as per the loading steps before give it to writer or sorter
    val updatedRdd = convertData(
      rdd,
      sparkSession,
      loadModel,
      isDataFrame,
      partitionValues)
    var attributes = curAttributes.map(a => {
      catalogAttributes.find(_.name.equalsIgnoreCase(a.name)).get
    })
    attributes = attributes.map { attr =>
      // Update attribute datatypes in case of dictionary columns, in case of dictionary columns
      // datatype is always int
      val column = table.getColumnByName(attr.name)
      val updatedDataType = if (column.hasEncoding(Encoding.DICTIONARY)) {
        IntegerType
      } else {
        attr.dataType match {
          case TimestampType | DateType =>
            LongType
          case _: StructType | _: ArrayType | _: MapType =>
            BinaryType
          case _ =>
            attr.dataType
        }
      }
      CarbonToSparkAdapter.createAttributeReference(attr.name,
        updatedDataType,
        attr.nullable,
        attr.metadata,
        attr.exprId,
        attr.qualifier,
        attr)
    }
    // Only select the required columns
    var output = if (partition.nonEmpty) {
      val lowerCasePartition = partition.map { case (key, value) => (key.toLowerCase, value) }
      catalogTable.schema.map { attr =>
        attributes.find(_.name.equalsIgnoreCase(attr.name)).get
      }.filter(attr => lowerCasePartition.getOrElse(attr.name.toLowerCase, None).isEmpty)
    } else {
      catalogTable.schema.map(f => attributes.find(_.name.equalsIgnoreCase(f.name)).get)
    }
    // Rearrange the partition column at the end of output list
    if (catalogTable.partitionColumnNames.nonEmpty &&
        (loadModel.getCarbonDataLoadSchema.getCarbonTable.isChildTableForMV) && output.nonEmpty) {
      val partitionOutPut =
        catalogTable.partitionColumnNames.map(col => output.find(_.name.equalsIgnoreCase(col)).get)
      output = output.filterNot(partitionOutPut.contains(_)) ++ partitionOutPut
    }
    val partitionsLen = rdd.partitions.length

    // If it is global sort scope then appl sort logical plan on the sort columns
    if (sortScope == SortScopeOptions.SortScope.GLOBAL_SORT) {
      // Because if the number of partitions greater than 1, there will be action operator(sample)
      // in sortBy operator. So here we cache the rdd to avoid do input and convert again.
      if (partitionsLen > 1) {
        updatedRdd.persist(StorageLevel.fromString(
          CarbonProperties.getInstance().getGlobalSortRddStorageLevel))
      }
      var numPartitions =
        CarbonDataProcessorUtil.getGlobalSortPartitions(loadModel.getGlobalSortPartitions)
      if (numPartitions <= 0) {
        numPartitions = partitionsLen
      }
      val sortColumns = attributes.take(table.getSortColumns().size())
      val dataTypes = sortColumns.map(_.dataType)
      val sortedRDD: RDD[InternalRow] =
        GlobalSortHelper.sortBy(updatedRdd, numPartitions, dataTypes)
      val outputOrdering = sortColumns.map(SortOrder(_, Ascending))
      (
        Project(
          output,
          LogicalRDD(attributes, sortedRDD, outputOrdering = outputOrdering)(sparkSession)
        ),
        partitionsLen,
        Some(updatedRdd)
      )
    } else {
      (
        Project(output, LogicalRDD(attributes, updatedRdd)(sparkSession)),
        partitionsLen,
        None
      )
    }
  }

  /**
   * Transform the rdd to logical plan as per the sortscope. If it is global sort scope then it
   * will convert to sort logical plan otherwise project plan.
   */
  def transformQueryWithInternalRow(rdd: RDD[InternalRow],
      sparkSession: SparkSession,
      loadModel: CarbonLoadModel,
      partitionValues: Array[String],
      catalogTable: CatalogTable,
      curAttributes: Seq[AttributeReference],
      sortScope: SortScopeOptions.SortScope,
      table: CarbonTable,
      partition: Map[String, Option[String]]): (LogicalPlan, Int, Option[RDD[InternalRow]]) = {
    // keep partition column to end if exists
    var colSchema = table.getTableInfo
      .getFactTable
      .getListOfColumns
      .asScala
    if (table.getPartitionInfo != null) {
      colSchema = colSchema.filterNot(x => x.isInvisible || x.getColumnName.contains(".") ||
                                           x.getSchemaOrdinal == -1 ||
                                           table.getPartitionInfo.getColumnSchemaList.contains(x))
      colSchema = colSchema ++ table
        .getPartitionInfo
        .getColumnSchemaList.toArray(new Array[ColumnSchema](table
        .getPartitionInfo
        .getColumnSchemaList.size()))
        .asInstanceOf[Array[ColumnSchema]]
    } else {
      colSchema = colSchema.filterNot(x => x.isInvisible || x.getColumnName.contains(".") ||
                                           x.getSchemaOrdinal == -1)
    }
    val updatedRdd: RDD[InternalRow] = CommonLoadUtils.getConvertedInternalRow(colSchema, rdd)
    val catalogAttributes = catalogTable.schema.toAttributes
    var attributes = curAttributes.map(a => {
      catalogAttributes.find(_.name.equalsIgnoreCase(a.name)).get
    })
    attributes = attributes.map { attr =>
      // Update attribute datatypes in case of dictionary columns, in case of dictionary columns
      // datatype is always int
      val column = table.getColumnByName(attr.name)
      val updatedDataType = if (column.hasEncoding(Encoding.DICTIONARY)) {
        IntegerType
      } else {
        attr.dataType match {
          case TimestampType | DateType =>
            LongType
          // complex type will be converted in CarbonOutputWriter.writeCarbon
          case _ =>
            attr.dataType
        }
      }
      CarbonToSparkAdapter.createAttributeReference(attr.name,
        updatedDataType,
        attr.nullable,
        attr.metadata,
        attr.exprId,
        attr.qualifier,
        attr)
    }
    // Only select the required columns
    var output = if (partition.nonEmpty) {
      val lowerCasePartition = partition.map { case (key, value) => (key.toLowerCase, value) }
      catalogTable.schema.map { attr =>
        attributes.find(_.name.equalsIgnoreCase(attr.name)).get
      }.filter(attr => lowerCasePartition.getOrElse(attr.name.toLowerCase, None).isEmpty)
    } else {
      catalogTable.schema.map(f => attributes.find(_.name.equalsIgnoreCase(f.name)).get)
    }
    // Rearrange the partition column at the end of output list
    if (catalogTable.partitionColumnNames.nonEmpty &&
        (loadModel.getCarbonDataLoadSchema.getCarbonTable.isChildTableForMV) && output.nonEmpty) {
      val partitionOutPut =
        catalogTable.partitionColumnNames.map(col => output.find(_.name.equalsIgnoreCase(col)).get)
      output = output.filterNot(partitionOutPut.contains(_)) ++ partitionOutPut
    }
    // Remove the thread local entries of previous configurations.
    DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
    val partitionsLen = rdd.partitions.length

    // If it is global sort scope then appl sort logical plan on the sort columns
    if (sortScope == SortScopeOptions.SortScope.GLOBAL_SORT) {
      // Because if the number of partitions greater than 1, there will be action operator(sample)
      // in sortBy operator. So here we cache the rdd to avoid do input and convert again.
      if (partitionsLen > 1) {
        updatedRdd.persist(StorageLevel.fromString(
          CarbonProperties.getInstance().getGlobalSortRddStorageLevel))
      }
      var numPartitions =
        CarbonDataProcessorUtil.getGlobalSortPartitions(loadModel.getGlobalSortPartitions)
      if (numPartitions <= 0) {
        numPartitions = partitionsLen
      }
      val sortColumns = attributes.take(table.getSortColumns().size())
      val dataTypes = sortColumns.map(_.dataType)
      val sortedRDD: RDD[InternalRow] =
        GlobalSortHelper.sortBy(updatedRdd, numPartitions, dataTypes)
      val outputOrdering = sortColumns.map(SortOrder(_, Ascending))
      (
        Project(
          output,
          LogicalRDD(attributes, sortedRDD, outputOrdering = outputOrdering)(sparkSession)
        ),
        partitionsLen,
        Some(updatedRdd)
      )
    } else {
      (
        Project(output, LogicalRDD(attributes, updatedRdd)(sparkSession)),
        partitionsLen,
        None
      )
    }
  }

  /**
   * Create the logical plan for update scenario. Here we should drop the segmentId column from the
   * input rdd.
   */
  def getLogicalQueryForUpdate(
      sparkSession: SparkSession,
      catalogTable: CatalogTable,
      df: DataFrame,
      carbonLoadModel: CarbonLoadModel): LogicalPlan = {
    SparkUtil.setNullExecutionId(sparkSession)
    // In case of update, we don't need the segmrntid column in case of partitioning
    val dropAttributes = df.logicalPlan.output.dropRight(1)
    val finalOutput = catalogTable.schema.map { attr =>
      dropAttributes.find { d =>
        val index = d.name.lastIndexOf(CarbonCommonConstants.UPDATED_COL_EXTENSION)
        if (index > 0) {
          d.name.substring(0, index).equalsIgnoreCase(attr.name)
        } else {
          d.name.equalsIgnoreCase(attr.name)
        }
      }.get
    }
    carbonLoadModel.setCsvHeader(catalogTable.schema.map(_.name.toLowerCase).mkString(","))
    carbonLoadModel.setCsvHeaderColumns(carbonLoadModel.getCsvHeader.split(","))
    Project(finalOutput, df.logicalPlan)
  }

  def convertToLogicalRelation(
      catalogTable: CatalogTable,
      sizeInBytes: Long,
      overWrite: Boolean,
      loadModel: CarbonLoadModel,
      sparkSession: SparkSession,
      operationContext: OperationContext,
      partition: Map[String, Option[String]],
      updateModel: Option[UpdateTableModel],
      optionsOriginal: mutable.Map[String, String],
      currPartitions: util.List[PartitionSpec]): LogicalRelation = {
    val table = loadModel.getCarbonDataLoadSchema.getCarbonTable
    val metastoreSchema = if (optionsOriginal.contains("no_rearrange_of_rows")) {
      StructType(catalogTable.schema.fields.map{f =>
        val column = table.getColumnByName(f.name)
        val updatedDataType = if (column.hasEncoding(Encoding.DICTIONARY)) {
          IntegerType
        } else {
          f.dataType match {
            case TimestampType | DateType =>
              LongType
            // complex type will be converted in CarbonOutputWriter.writeCarbon
            case _ =>
              f.dataType
          }
        }
        f.copy(dataType = updatedDataType)
      })
    } else {
      StructType(catalogTable.schema.fields.map{f =>
        val column = table.getColumnByName(f.name)
        val updatedDataType = if (column.hasEncoding(Encoding.DICTIONARY)) {
          IntegerType
        } else {
          f.dataType match {
            case TimestampType | DateType =>
              LongType
            case _: StructType | _: ArrayType | _: MapType =>
              BinaryType
            case _ =>
              f.dataType
          }
        }
        f.copy(dataType = updatedDataType)
      })
    }
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
    if (updateModel.isDefined) {
      options += (("updatetimestamp", updateModel.get.updatedTimeStamp.toString))
      if (updateModel.get.deletedSegments.nonEmpty) {
        options += (("segmentsToBeDeleted",
          updateModel.get.deletedSegments.map(_.getSegmentNo).mkString(",")))
      }
    }
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

    if (options.contains("no_rearrange_of_rows")) {
      CarbonReflectionUtils.getLogicalRelation(hdfsRelation,
        metastoreSchema.toAttributes,
        Some(catalogTable),
        false)
    } else {
      CarbonReflectionUtils.getLogicalRelation(hdfsRelation,
        hdfsRelation.schema.toAttributes,
        Some(catalogTable),
        false)
    }
  }

  def getConvertedInternalRow(columnSchema: Seq[ColumnSchema],
      rdd: RDD[InternalRow]): RDD[InternalRow] = {
    // Converts the data as per the loading steps before give it to writer or sorter
    var timeStampIndex = scala.collection.mutable.Set[Int]()
    var directDictionaryIndex = scala.collection.mutable.Set[Int]()
    var i: Int = 0
    for (col <- columnSchema) {
      if (col.getDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.TIMESTAMP) {
        timeStampIndex += i
      }
      if (col.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        directDictionaryIndex += i;
      }
      i = i + 1
    }
    val updatedRdd: RDD[InternalRow] = rdd.map { internalRow =>
      for (index <- timeStampIndex) {
        internalRow.setLong(index, internalRow.getLong(index) / 1000)
      }
      for (index <- directDictionaryIndex) {
        if (internalRow.isNullAt(index)) {
          // null values must be replaced with direct dictionary null values
          internalRow.setInt(index, CarbonCommonConstants.DIRECT_DICT_VALUE_NULL)
        } else {
          internalRow.setInt(index,
            internalRow.getInt(index) + DateDirectDictionaryGenerator.cutOffDate)
        }
      }
      internalRow
    }
    updatedRdd
  }

  def getTimeAndDateFormatFromLoadModel(loadModel: CarbonLoadModel): (SimpleDateFormat,
    SimpleDateFormat) = {
    var timeStampformatString = loadModel.getTimestampformat
    if (timeStampformatString.isEmpty) {
      timeStampformatString = loadModel.getDefaultTimestampFormat
    }
    val timeStampFormat = new SimpleDateFormat(timeStampformatString)
    var dateFormatString = loadModel.getDateFormat
    if (dateFormatString.isEmpty) {
      dateFormatString = loadModel.getDefaultDateFormat
    }
    val dateFormat = new SimpleDateFormat(dateFormatString)
    (timeStampFormat, dateFormat)
  }


}
