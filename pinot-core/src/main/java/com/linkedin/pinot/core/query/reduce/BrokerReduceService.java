/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.reduce;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.query.ReduceService;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.request.Selection;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.response.broker.AggregationResult;
import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import com.linkedin.pinot.common.response.broker.QueryProcessingException;
import com.linkedin.pinot.common.response.broker.SelectionResults;
import com.linkedin.pinot.common.utils.DataTable;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.query.aggregation.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByOperatorService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorService;
import com.linkedin.pinot.core.query.selection.SelectionOperatorUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>BrokerReduceService</code> class provides service to reduce data tables gathered from multiple servers
 * to {@link BrokerResponseNative}.
 */
public class BrokerReduceService implements ReduceService<BrokerResponseNative> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerReduceService.class);

  @Override
  public BrokerResponseNative reduceOnDataTable(@Nonnull BrokerRequest brokerRequest,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap) {
    if (dataTableMap.size() == 0) {
      // Empty response.
      return BrokerResponseNative.empty();
    }

    BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
    List<QueryProcessingException> processingExceptions = brokerResponseNative.getProcessingExceptions();
    long numDocsScanned = 0L;
    long numEntriesScannedInFilter = 0L;
    long numEntriesScannedPostFilter = 0L;
    long numTotalRawDocs = 0L;

    // Process server response metadata.
    Map.Entry<ServerInstance, DataTable> entryWithDataSchema = null;
    Iterator<Map.Entry<ServerInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerInstance, DataTable> entry = iterator.next();
      ServerInstance serverInstance = entry.getKey();
      DataTable dataTable = entry.getValue();
      Map<String, String> metadata = dataTable.getMetadata();

      // Reduce on trace info.
      if (brokerRequest.isEnableTrace()) {
        brokerResponseNative.getTraceInfo()
            .put(serverInstance.getHostname(), metadata.get(DataTable.TRACE_INFO_METADATA_KEY));
      }

      // Reduce on exceptions.
      for (String key : metadata.keySet()) {
        if (key.startsWith(DataTable.EXCEPTION_METADATA_KEY)) {
          processingExceptions.add(new QueryProcessingException(Integer.parseInt(key.substring(9)), metadata.get(key)));
        }
      }

      // Reduce on execution statistics.
      String numDocsScannedString = metadata.get(DataTable.NUM_DOCS_SCANNED_METADATA_KEY);
      if (numDocsScannedString != null) {
        numDocsScanned += Long.parseLong(numDocsScannedString);
      }
      String numEntriesScannedInFilterString = metadata.get(DataTable.NUM_ENTRIES_SCANNED_IN_FILTER_METADATA_KEY);
      if (numEntriesScannedInFilterString != null) {
        numEntriesScannedInFilter += Long.parseLong(numEntriesScannedInFilterString);
      }
      String numEntriesScannedPostFilterString = metadata.get(DataTable.NUM_ENTRIES_SCANNED_POST_FILTER_METADATA_KEY);
      if (numEntriesScannedPostFilterString != null) {
        numEntriesScannedPostFilter += Long.parseLong(numEntriesScannedPostFilterString);
      }
      String numTotalRawDocsString = metadata.get(DataTable.TOTAL_DOCS_METADATA_KEY);
      if (numTotalRawDocsString != null) {
        numTotalRawDocs += Long.parseLong(numTotalRawDocsString);
      }

      // After processing the metadata, remove data tables without data rows inside.
      if (dataTable.getNumberOfRows() == 0) {
        if (entryWithDataSchema == null && dataTable.getDataSchema() != null) {
          entryWithDataSchema = entry;
        }
        iterator.remove();
      }
    }

    // Set execution statistics.
    brokerResponseNative.setNumDocsScanned(numDocsScanned);
    brokerResponseNative.setNumEntriesScannedInFilter(numEntriesScannedInFilter);
    brokerResponseNative.setNumEntriesScannedPostFilter(numEntriesScannedPostFilter);
    brokerResponseNative.setTotalDocs(numTotalRawDocs);

    // If all data tables have no data row inside, leave one with data schema to construct empty results.
    if (dataTableMap.isEmpty() && entryWithDataSchema != null) {
      dataTableMap.put(entryWithDataSchema.getKey(), entryWithDataSchema.getValue());
    }

    // Attach query results to the broker response.
    if (!dataTableMap.isEmpty()) {
      // Reduce server response data.
      if (brokerRequest.isSetSelections()) {
        // Selection query.
        attachSelectionResults(brokerResponseNative, brokerRequest.getSelections(), dataTableMap);
      } else {
        // Aggregation query.
        List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
        if (!brokerRequest.isSetGroupBy()) {
          // Aggregation only query.
          attachAggregationResults(brokerResponseNative, aggregationsInfo, dataTableMap);
        } else {
          // Aggregation group-by query.
          attachGroupByResults(brokerResponseNative, aggregationsInfo, brokerRequest.getGroupBy(), dataTableMap);
        }
      }
    }

    return brokerResponseNative;
  }

  /**
   * Reduce selection results from multiple servers and attach them into BrokerResponseNative.
   *
   * @param brokerResponseNative broker response to be attached.
   * @param selection selection information.
   * @param dataTableMap map from server to data table.
   */
  private void attachSelectionResults(@Nonnull BrokerResponseNative brokerResponseNative, @Nonnull Selection selection,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap) {
    DataSchema dataSchema = dataTableMap.values().iterator().next().getDataSchema();

    // Remove conflicting responses.
    List<String> droppedServers = removeConflictingResponses(dataSchema, dataTableMap);
    if (!droppedServers.isEmpty()) {
      String errorMessage =
          QueryException.MERGE_RESPONSE_ERROR.getMessage() + ": responses from servers: " + droppedServers
              + " got dropped due to data schema mismatch.";
      LOGGER.error(errorMessage);
      brokerResponseNative.addToExceptions(
          new QueryProcessingException(QueryException.MERGE_RESPONSE_ERROR_CODE, errorMessage));
    }

    // Reduce the selection results.
    SelectionResults selectionResults;
    if (selection.isSetSelectionSortSequence()) {
      // Selection order-by.
      SelectionOperatorService selectionService = new SelectionOperatorService(selection, dataSchema);
      selectionResults =
          selectionService.renderSelectionResultsWithOrdering(selectionService.reduceWithOrdering(dataTableMap));
    } else {
      // Selection only.
      selectionResults = SelectionOperatorUtils.renderSelectionResultsWithoutOrdering(
          SelectionOperatorUtils.reduceWithoutOrdering(dataTableMap, selection.getSize()),
          selection.getSelectionColumns(), dataSchema);
    }

    brokerResponseNative.setSelectionResults(selectionResults);
  }

  /**
   * Given a data schema, remove data tables that do not match this data schema.
   *
   * @param dataSchema data schema.
   * @param dataTableMap map from server to data table.
   * @return list of server names where the data table got removed.
   */
  private List<String> removeConflictingResponses(@Nonnull DataSchema dataSchema,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap) {
    List<String> droppedServers = new ArrayList<>();
    Iterator<Map.Entry<ServerInstance, DataTable>> iterator = dataTableMap.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<ServerInstance, DataTable> entry = iterator.next();
      DataSchema dataSchemaToCompare = entry.getValue().getDataSchema();
      if (!dataSchema.equals(dataSchemaToCompare)) {
        droppedServers.add(entry.getKey().toString());
        iterator.remove();
      }
    }
    return droppedServers;
  }

  /**
   * Reduce aggregation results from multiple servers and attach them into BrokerResponseNative.
   *
   * @param brokerResponseNative broker response to be attached.
   * @param aggregationsInfo list of aggregation information.
   * @param dataTableMap map from server to data table.
   */
  private void attachAggregationResults(@Nonnull BrokerResponseNative brokerResponseNative,
      @Nonnull List<AggregationInfo> aggregationsInfo, @Nonnull Map<ServerInstance, DataTable> dataTableMap) {
    int numAggregations = aggregationsInfo.size();
    List<List<Serializable>> shuffledAggregationResults = shuffleAggregationResults(aggregationsInfo, dataTableMap);
    List<AggregationResult> reducedAggregationResults = new ArrayList<>(numAggregations);
    List<AggregationFunction> aggregationFunctions =
        AggregationFunctionFactory.getAggregationFunction(aggregationsInfo);

    for (int i = 0; i < numAggregations; i++) {
      String aggregationFunction = aggregationFunctions.get(i).getFunctionName();
      String formattedValue = AggregationGroupByOperatorService.formatValue(
          aggregationFunctions.get(i).reduce(shuffledAggregationResults.get(i)));
      AggregationResult aggregationResult = new AggregationResult(aggregationFunction, formattedValue);
      reducedAggregationResults.add(aggregationResult);
    }

    brokerResponseNative.setAggregationResults(reducedAggregationResults);
  }

  /**
   * Shuffle aggregation results, gather all results for each aggregation function together.
   *
   * @param aggregationsInfo list of aggregation information.
   * @param dataTableMap map from server to data table.
   * @return shuffled aggregation results.
   */
  private List<List<Serializable>> shuffleAggregationResults(@Nonnull List<AggregationInfo> aggregationsInfo,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap) {
    int numAggregations = aggregationsInfo.size();
    int numDataTables = dataTableMap.size();
    List<List<Serializable>> shuffledAggregationResults = new ArrayList<>(numAggregations);
    for (int i = 0; i < numAggregations; i++) {
      shuffledAggregationResults.add(new ArrayList<Serializable>(numDataTables));
    }

    for (DataTable dataTable : dataTableMap.values()) {
      DataSchema dataSchema = dataTable.getDataSchema();
      for (int i = 0; i < numAggregations; i++) {
        FieldSpec.DataType columnType = dataSchema.getColumnType(i);
        switch (columnType) {
          case LONG:
            shuffledAggregationResults.get(i).add(dataTable.getLong(0, i));
            break;
          case DOUBLE:
            shuffledAggregationResults.get(i).add(dataTable.getDouble(0, i));
            break;
          case OBJECT:
            shuffledAggregationResults.get(i).add(dataTable.getObject(0, i));
            break;
          default:
            throw new IllegalStateException("Illegal column type in aggregation results: " + columnType);
        }
      }
    }

    return shuffledAggregationResults;
  }

  /**
   * Reduce group-by results from multiple servers and attach them into BrokerResponseNative.
   *
   * @param brokerResponseNative broker response to be attached.
   * @param aggregationsInfo list of aggregation information.
   * @param groupBy group-by information.
   * @param dataTableMap map from server to data table.
   */
  private void attachGroupByResults(@Nonnull BrokerResponseNative brokerResponseNative,
      @Nonnull List<AggregationInfo> aggregationsInfo, @Nonnull GroupBy groupBy,
      @Nonnull Map<ServerInstance, DataTable> dataTableMap) {
    AggregationGroupByOperatorService aggregationGroupByOperatorService =
        new AggregationGroupByOperatorService(aggregationsInfo, groupBy);
    List<AggregationResult> aggregationResults = aggregationGroupByOperatorService.renderAggregationGroupByResult(
        aggregationGroupByOperatorService.reduceGroupByOperators(dataTableMap));
    brokerResponseNative.setAggregationResults(aggregationResults);
  }
}
