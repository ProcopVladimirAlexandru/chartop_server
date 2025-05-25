import structlog
from contextlib import asynccontextmanager
from chartop_server.controllers.tsdb.exceptions import TSDBControllerException
from chartop_server.models import (
    TimeseriesResponse,
    MultipleTimeseriesExternal,
    SingleTimeseriesExternal,
    TagsResponse,
    MetricsResponse,
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

    async def get_timeseries(
        self,
        page_number: int,
        page_size: int,
        order_by: int,
        order_asc: bool = True,
        tags: list[int] = None,
        all_or_any_tags: AllOrAnyTags = AllOrAnyTags.ANY,
    ) -> TimeseriesResponse:
        async with self.connect() as conn:
            single_ts_externals: list[SingleTimeseriesExternal] = list()
            try:
                meta_models = await self._connector.get_filtered_metadata(
                    conn=conn,
                    order_by_metric_uid=order_by,
                    order_asc=order_asc,
                    tag_uids=tags,
                    all_or_any_tags=all_or_any_tags,
                    limit=page_size,
                    offset=page_number * page_size,
                )
                ts_uids = [m.uid for m in meta_models]
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
                    conn=conn, ts_uids=ts_uids
                )
                ts_to_metric_models_per_ts_uid = group_by(
                    ts_to_metric_models, self._connector.ts_to_metric_ts_uid_col.lower()
                )
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to filter timeseries.", http_status_code=500
                ) from ex

            try:
                ts_models = await self._connector.get_timeseries(
                    conn=conn, ts_uids=ts_uids, order_asc=True
                )
                ts_models_by_uid = group_by(ts_models, "uid")
                for meta_model in meta_models:
                    ts_models = ts_models_by_uid[meta_model.uid]
                    single_ts_externals.append(
                        SingleTimeseriesExternal.from_db_models(
                            meta_model,
                            ts_models,
                            ts_to_tag_models_per_ts_uid.get(meta_model.uid, []),
                            ts_to_metric_models_per_ts_uid.get(meta_model.uid, []),
                        )
                    )
            except Exception as ex:
                raise TSDBControllerException(
                    message="Failed to filter timeseries.", http_status_code=500
                ) from ex

            return TimeseriesResponse(
                success=True,
                message="Successfully retrieved timeseries.",
                data=MultipleTimeseriesExternal(single_timeseries=single_ts_externals),
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
