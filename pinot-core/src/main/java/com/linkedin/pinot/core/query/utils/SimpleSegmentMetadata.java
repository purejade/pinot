package com.linkedin.pinot.core.query.utils;

import org.apache.commons.configuration.Configuration;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.SegmentMetadata;


public class SimpleSegmentMetadata implements SegmentMetadata {

  private static final String SEGMENT_SIZE = "segment.size";
  private static final String SEGMENT_RESOURCE_NAME = "segment.resource.name";
  private static final String SEGMENT_TABLE_NAME = "segment.table.name";

  private String _resourceName;
  private String _tableName;
  private String _indexType;
  private Duration _timeGranularity;
  private Interval _interval;
  private String _crc;
  private String _version;
  private Schema _schema;
  private String _shardingKey;
  private long _size;

  @Override
  public String getResourceName() {
    return _resourceName;
  }

  @Override
  public String getTableName() {
    return _tableName;
  }

  @Override
  public String getIndexType() {
    return _indexType;
  }

  @Override
  public Duration getTimeGranularity() {
    return _timeGranularity;
  }

  @Override
  public Interval getTimeInterval() {
    return _interval;
  }

  @Override
  public String getCrc() {
    return _crc;
  }

  @Override
  public String getVersion() {
    return _version;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public String getShardingKey() {
    return _shardingKey;
  }

  public void setSize(long size) {
    _size = size;
  }

  public static SegmentMetadata load(Configuration properties) {
    SegmentMetadata segmentMetadata = new SimpleSegmentMetadata();
    //segmentMetadata.setResourceName(properties.getString(SEGMENT_RESOURCE_NAME, "defaultResource"));
    //    /segmentMetadata.setTableName(properties.getString(SEGMENT_TABLE_NAME, "defaultTable"));
    ((SimpleSegmentMetadata) segmentMetadata).setSize(properties.getLong(SEGMENT_SIZE, 0));
    return segmentMetadata;
  }

  @Override
  public int getTotalDocs() {
    return (int) _size;
  }

  @Override
  public String getIndexDir() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }

}
