import datetime
from collections import defaultdict

import structlog
from contextlib import asynccontextmanager
from chartop_server.controllers.tsdb.exceptions import TSDBControllerException
from chartop_server.models import (
    ChartopResponse,
    ChartopExternal,
    SingleTimeseriesExternal,
    TagsResponse,
    MetricsResponse,
    VisualizationVectorsResponse,
    VisualizationVectorsWithOriginExternal,
)
from chartop_server.models.models import (
    ChartopEntryExternal,
    TSWithVisualizationVectorExternal,
)
from chartop_server.utils import group_by

from pva_tsdb_connector.postgres_connector.connector import (
    AsyncPostgresSQLAlchemyCoreConnector,
)
from pva_tsdb_connector.postgres_connector.configs import (
    ConnectorSettings,
    ConnectionSettings,
)
from pva_tsdb_connector.enums import AllOrAnyTags
from pva_tsdb_connector.models import (
    TSDataModel,
    TSToMetricModel,
    MetricValueWithOperands,
    TSWithVisualizationVectorModel,
)


class TSDBController:
    def __init__(self, connection_settings: ConnectionSettings):
        self._connection_settings: ConnectionSettings = connection_settings
        self._connector_settings: ConnectorSettings = ConnectorSettings(
            connection=self._connection_settings
        )
        self._connector: AsyncPostgresSQLAlchemyCoreConnector = (
            AsyncPostgresSQLAlchemyCoreConnector(self._connector_settings)
        )
        self._logger = structlog.getLogger(component="TSDBController")

    async def init(self):
        await self._init_connector()

    async def _init_connector(self):
        await self._connector.connect()
        self._logger.info("Initialized TSDBController's TSDBConnector")

    async def get_chartop(
        self,
        page_number: int,
        page_size: int,
        order_by: int,
        order_asc: bool = True,
        tags: list[int] | None = None,
        all_or_any_tags: AllOrAnyTags = AllOrAnyTags.ANY,
    ) -> ChartopResponse:
        async with self.connect() as conn:
            try:
                chartop: list[
                    MetricValueWithOperands
                ] = await self._connector.get_ordered_values_and_operands(
                    conn=conn,
                    order_by_metric_uid=order_by,
                    order_asc=order_asc,
                    tag_uids=tags,
                    all_or_any_tags=all_or_any_tags,
                    limit=page_size,
                    offset=page_number * page_size,
                )
                ts_uids = [op.uid for m in chartop for op in m.operands]
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to filter timeseries.", http_status_code=500
                ) from ex

            try:
                ts_to_tag_models = await self._connector.get_ts_to_tags(
                    conn=conn, ts_uids=ts_uids
                )
                ts_to_tag_models_per_ts_uid = group_by(
                    ts_to_tag_models, self._connector.ts_to_tag_ts_uid_col.lower()
                )
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to filter timeseries.", http_status_code=500
                ) from ex

            try:
                ts_to_metric_models = await self._connector.get_ts_to_metrics(
                    conn=conn, ts_uids=ts_uids, metric_uids=list(range(1, 16))
                )
                ts_to_metric_models_per_ts_uid: dict[int, list[TSToMetricModel]] = (
                    defaultdict(list)
                )
                for m in ts_to_metric_models:
                    ts_to_metric_models_per_ts_uid[m.ts_uids[0]].append(m)
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to filter timeseries.", http_status_code=500
                ) from ex

            try:
                ts_models: list[TSDataModel] = await self._connector.get_timeseries(
                    conn=conn, ts_uids=ts_uids, order_asc=True
                )
                ts_models_by_uid = group_by(ts_models, "uid")
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to filter timeseries.", http_status_code=500
                ) from ex

            try:
                ts_uids_with_vv: set[int] = set(
                    await self._connector.get_ts_uids_with_vv(
                        conn=conn,
                        ts_uids=ts_uids,
                    )
                )
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to filter timeseries.", http_status_code=500
                ) from ex

        chartop_external: list[ChartopEntryExternal] = list()
        for chartop_entry in chartop:
            external_operands: list[SingleTimeseriesExternal] = list()
            for meta_model in chartop_entry.operands:
                external_operands.append(
                    SingleTimeseriesExternal.from_db_models(
                        meta_model=meta_model,
                        ts_models=ts_models_by_uid[meta_model.uid],
                        ts_to_tag_models=ts_to_tag_models_per_ts_uid.get(
                            meta_model.uid, []
                        ),
                        ts_to_metric_models=ts_to_metric_models_per_ts_uid.get(
                            meta_model.uid, []
                        ),
                        ts_uids_with_vv=ts_uids_with_vv,
                    )
                )
            chartop_external.append(
                ChartopEntryExternal(
                    operands=external_operands,
                    order_by_metric_value=chartop_entry.metric_value,
                )
            )
        return ChartopResponse(
            success=True,
            message="Successfully retrieved timeseries.",
            data=ChartopExternal(
                chartop_entries=chartop_external,
                order_by_metric_uid=order_by,
            ),
        )

    async def get_visualization_vectors(
        self,
        origin_vector: list[float] | None,
        origin_ts_uid: int | None,
        radius: float,
        limit: int,
        exclude_ts_uids: list[int] | None = None,
        start_date: datetime.datetime | None = None,
    ) -> VisualizationVectorsResponse:
        if (origin_vector is None and origin_ts_uid is None) or (
            origin_vector is not None and origin_ts_uid is not None
        ):
            raise TSDBControllerException(
                message="Specify exactly one of 'origin_vector', 'origin_ts_uid'.",
                http_status_code=400,
            )

        async with self.connect() as conn:
            try:
                ts_with_vectors: list[
                    TSWithVisualizationVectorModel
                ] = await self._connector.get_ts_with_visualization_vector(
                    conn=conn,
                    origin_vector=origin_vector,
                    origin_ts_uid=origin_ts_uid,
                    radius=radius,
                    limit=limit,
                    exclude_ts_uids=exclude_ts_uids,
                )
                ts_uids = [m.metadata.uid for m in ts_with_vectors]
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to get timeseries with visualization vectors.",
                    http_status_code=500,
                ) from ex

            try:
                ts_to_metric_models = await self._connector.get_ts_to_metrics(
                    conn=conn, ts_uids=ts_uids, metric_uids=list(range(1, 16))
                )
                ts_to_metric_models_per_ts_uid: dict[int, list[TSToMetricModel]] = (
                    defaultdict(list)
                )
                for m in ts_to_metric_models:
                    ts_to_metric_models_per_ts_uid[m.ts_uids[0]].append(m)
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to get timeseries with visualization vectors.",
                    http_status_code=500,
                ) from ex

            try:
                ts_models: list[TSDataModel] = await self._connector.get_timeseries(
                    conn=conn, ts_uids=ts_uids, order_asc=True, start_date=start_date
                )
                ts_models_by_uid = group_by(ts_models, "uid")
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to get timeseries with visualization vectors.",
                    http_status_code=500,
                ) from ex

        origin: list[float] | None = None
        ts_with_visualization_vectors: list[TSWithVisualizationVectorExternal] = list()
        for entry in ts_with_vectors:
            single_ts = SingleTimeseriesExternal.from_db_models(
                meta_model=entry.metadata,
                ts_models=ts_models_by_uid.get(entry.metadata.uid, list()),
                ts_to_tag_models=None,
                ts_to_metric_models=ts_to_metric_models_per_ts_uid.get(
                    entry.metadata.uid, list()
                ),
            )
            ts_with_visualization_vectors.append(
                TSWithVisualizationVectorExternal(
                    timestamps=single_ts.timestamps,
                    values=single_ts.values,
                    metadata=single_ts.metadata,
                    visualization_vector=entry.visualization_vector,
                )
            )
            if origin_ts_uid is not None and origin_ts_uid == entry.metadata.uid:
                origin = entry.visualization_vector

        if origin_ts_uid is not None and origin is None:
            raise TSDBControllerException(
                message=f"Failed to locate origin TS UID {origin_ts_uid}.",
                http_status_code=404,
            )

        return VisualizationVectorsResponse(
            success=True,
            message="Successfully retrieved visualization vectors.",
            data=VisualizationVectorsWithOriginExternal(
                ts_with_visualization_vectors=ts_with_visualization_vectors,
                origin=origin,
            ),
        )

    async def get_tags(self) -> TagsResponse:
        async with self.connect() as conn:
            try:
                tags = await self._connector.get_tags(conn=conn)
                return TagsResponse(
                    success=True, message="Successfully retrieved tags.", data=tags
                )
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to get tags.", http_status_code=500
                ) from ex

    async def get_metrics(self) -> MetricsResponse:
        async with self.connect() as conn:
            try:
                metrics = await self._connector.get_metrics(conn=conn)
                return MetricsResponse(
                    success=True,
                    message="Successfully retrieved metrics.",
                    data=metrics,
                )
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to get metrics.", http_status_code=500
                ) from ex

    async def cleanup(self):
        await self._connector.close()
        self._logger.info("Closed TSDBController's TSDBConnector")

    @asynccontextmanager
    async def connect(self):
        conn = await self._connector.get_connection()
        try:
            yield conn
        finally:
            # this will automatically roll back any uncommited changes
            await conn.close()
        # the user of connection should commit explicitly
