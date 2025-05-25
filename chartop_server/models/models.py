import datetime
import json
from typing import Any, Optional
from pydantic import BaseModel, Field

from pva_tsdb_connector.models import (
    TSDataModel,
    TSMetadataModel,
    TagModel,
    MetricModel,
    TSToTagModel,
    TSToMetricModel,
)


class BaseResponse(BaseModel):
    success: bool = Field(
        title="Success", description="Whether the operation was successful"
    )
    message: str = Field(
        title="Response Message",
        description="Relevant textual information about what the request achieved",
    )


class DataResponse(BaseResponse):
    data: Optional[Any] = Field(
        default=None,
        title="Data",
        description="Any data the server sends back to the caller",
    )


class SingleTSMetadataExternal(BaseModel):
    class Tag(BaseModel):
        uid: int = Field()

    class Metric(BaseModel):
        uid: int = Field()
        value: float = Field()
        data: dict | None = Field(default=None)

    timezone: int = Field(
        default=0, title="Offset From GMT", description="Seconds offset from GMT"
    )
    uid: int = Field(title="Timeseries Integer UID")
    name: str = Field(title="Timeseries Name")
    description: Optional[str] = Field(default=None, title="Timeseries Description")
    unit: Optional[str] = Field(default=None, title="Timeseries Measure Unit")
    source_uid: str = Field(title="Datasource UID")
    uid_from_source: str = Field(
        title="Timeseries UID from Source",
        description="UID given by data source to the timeseries",
    )
    successful_last_update_time: int = Field(title="Successful Last Update")
    tags: list[Tag] = Field(default=None, title="Tags")
    metrics: list[Metric] = Field(default=None, title="Metrics")


# class SingleTimeseriesDatapoint(BaseModel):
#     timestamp: int = Field(title="GMT Timestamp", description="GMT milliseconds since the epoch")
#     value: float = Field(title="Float Value", description="Float value corresponding to timestamp")


class SingleTimeseriesExternal(BaseModel):
    # data: list[SingleTimeseriesDatapoint] = Field(title="Single Timeseries Datapoint", description="All time variable data for this timeseries")
    timestamps: list[int] = Field(
        title="GMT Timestamps", description="GMT milliseconds since the epoch"
    )
    values: list[float] = Field(
        title="Float Values", description="Float values corresponding to timestamps"
    )
    metadata: SingleTSMetadataExternal = Field(
        title="TS Metadata", description="Metadata about this particular timeseries"
    )

    @staticmethod
    def from_db_models(
        meta_model: TSMetadataModel,
        ts_models: list[TSDataModel],
        ts_to_tag_models: list[TSToTagModel] = None,
        ts_to_metric_models: list[TSToMetricModel] = None,
    ):
        if not ts_to_tag_models:
            ts_to_tag_models = []
        if not ts_to_metric_models:
            ts_to_metric_models = []
        timestamps: list[int] = list()
        values: list[float] = list()
        for ts_model in ts_models:
            timestamps.append(int(ts_model.time.timestamp() * 1000))
            values.append(ts_model.value)

        timezone: int = 0
        if len(timestamps) > 0:
            timezone = int(
                datetime.datetime.utcoffset(ts_models[0].time).total_seconds() * 1000
            )

        return SingleTimeseriesExternal(
            timestamps=timestamps,
            values=values,
            metadata=SingleTSMetadataExternal(
                timezone=timezone,
                uid=meta_model.uid,
                name=meta_model.name,
                description=meta_model.description,
                unit=meta_model.unit,
                source_uid=meta_model.source_uid,
                uid_from_source=meta_model.uid_from_source,
                successful_last_update_time=int(
                    meta_model.successful_last_update_time.timestamp() * 1000
                ),
                tags=[
                    SingleTSMetadataExternal.Tag(uid=m.tag_uid)
                    for m in ts_to_tag_models
                ],
                metrics=[
                    SingleTSMetadataExternal.Metric(
                        uid=m.metric_uid,
                        value=m.value,
                        data=json.loads(m.data_json) if m.data_json else None,
                    )
                    for m in ts_to_metric_models
                ],
            ),
        )


class MultipleTSMetadataExternal(BaseModel):
    total_ts_count: int


class MultipleTimeseriesExternal(BaseModel):
    single_timeseries: list[SingleTimeseriesExternal] = Field(
        title="Single Timeseries Data", description="Data of individual timeseries"
    )
    multiple_timeseries_metadata: MultipleTSMetadataExternal = Field(
        default=None,
        title="Multiple Timeseries Metadata",
        description="Contains data regarding all single timeseries in this response",
    )


class TimeseriesResponse(DataResponse):
    data: MultipleTimeseriesExternal = Field(
        title="Main Timeseries Response",
        description="Contains both global data and data "
        "about each single timeseries that was queried",
    )


class TagsResponse(DataResponse):
    data: list[TagModel] = Field(
        title="All Available Tags",
        description="Returns all available tags that a timeseries can be tagged with.",
    )


class MetricsResponse(DataResponse):
    data: list[MetricModel] = Field(
        title="All Available Metrics",
        description="Returns all available metrics that can be computed for timeseries.",
    )
