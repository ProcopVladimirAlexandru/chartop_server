from pva_tsdb_connector.postgres_connector.configs import ConnectionSettings
from chartop_server.controllers.tsdb.controller import TSDBController


class TSDBControllerContainer:
    controller_config: ConnectionSettings | None = None
    controller: TSDBController | None = None
    initialized: bool = False

    @staticmethod
    async def init_controller(controller_config: ConnectionSettings | None = None):
        if controller_config is None:
            controller_config = ConnectionSettings()  # type: ignore
        TSDBControllerContainer.controller_config = controller_config
        TSDBControllerContainer.controller = TSDBController(
            connection_settings=TSDBControllerContainer.controller_config
        )
        await TSDBControllerContainer.controller.init()
        TSDBControllerContainer.initialized = True

    @staticmethod
    def get_controller():
        if not TSDBControllerContainer.initialized:
            raise RuntimeError("Controller not initialized")
        return TSDBControllerContainer.controller
