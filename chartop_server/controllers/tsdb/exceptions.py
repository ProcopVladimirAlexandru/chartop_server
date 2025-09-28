from typing import Any


class TSDBControllerException(Exception):
    def __init__(self, message: str, http_status_code: int, data: Any = None):
        super().__init__(message)
        self._http_status_code = http_status_code
        self._data = data

    @property
    def http_status_code(self) -> int:
        return self._http_status_code

    @http_status_code.setter
    def http_status_code(self, http_status_code: int) -> None:
        self._http_status_code = http_status_code

    @property
    def data(self) -> Any:
        return self._data

    @data.setter
    def data(self, data: Any) -> None:
        self._data = data
