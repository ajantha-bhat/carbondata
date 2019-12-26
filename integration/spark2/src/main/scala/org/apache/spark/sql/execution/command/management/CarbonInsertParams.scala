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

import java.util

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.command.UpdateTableModel
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.statusmanager.SegmentStatus
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/*
* intermediate object to pass between load functions
*/
case class CarbonInsertParams(sparkSession: SparkSession,
    carbonProperty: CarbonProperties,
    carbonLoadModel: CarbonLoadModel,
    partitionStatus: SegmentStatus,
    hadoopConf: Configuration,
    operationContext: OperationContext,
    updateModel: Option[UpdateTableModel],
    var dataFrame: DataFrame,
    isOverwriteTable: Boolean,
    logicalPartitionRelation: LogicalRelation,
    partition: Map[String, Option[String]],
    tableName: String,
    sizeInBytes: Long,
    optionsOriginal: mutable.Map[String, String],
    currPartitions: util.List[PartitionSpec]) {
}

case class CarbonInsertRddParams(sparkSession: SparkSession,
    carbonProperty: CarbonProperties,
    carbonLoadModel: CarbonLoadModel,
    partitionStatus: SegmentStatus,
    hadoopConf: Configuration,
    operationContext: OperationContext,
    scanResultRDD : RDD[InternalRow],
    isOverwriteTable: Boolean,
    logicalPartitionRelation: LogicalRelation,
    partition: Map[String, Option[String]],
    tableName: String,
    sizeInBytes: Long,
    optionsOriginal: mutable.Map[String, String],
    currPartitions: util.List[PartitionSpec]) {
}

case class CarbonLoadParams(sparkSession: SparkSession,
    carbonProperty: CarbonProperties,
    carbonLoadModel: CarbonLoadModel,
    partitionStatus: SegmentStatus,
    hadoopConf: Configuration,
    operationContext: OperationContext,
    isOverwriteTable: Boolean,
    logicalPartitionRelation: LogicalRelation,
    partition: Map[String, Option[String]],
    tableName: String,
    sizeInBytes: Long,
    optionsOriginal: mutable.Map[String, String],
    currPartitions: util.List[PartitionSpec]) {
}
