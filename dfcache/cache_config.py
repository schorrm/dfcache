import os
import pathlib
from typing import Optional, cast

from . import file_utils
from .utils import singleton


def _get_env_name(project_name: str) -> str:
    return f'{project_name.upper()}_ENV'

def _make_default(project_name: str) -> str:
    return os.path.join(pathlib.Path.home(), f'.{project_name}', 'cache')


class CacheConfig(singleton.Singleton):
    DEFAULT_EXPIRATION = 7  # number of days to cache by default
    DEFAULT_CACHE_TYPE = 'parquet'
    CFG_FILENAME = 'cache_config.json'

    def __init__(self, project_name: str, enabled: bool = False, default_expiration: int = None, cache_type: Optional[str] = None):
        self.enabled = enabled
        self.cache_directory = os.environ.get(_get_env_name(project_name)) or _make_default(project_name)
        self.default_expiration = default_expiration or CacheConfig.DEFAULT_EXPIRATION
        if cache_type is not None:
            self.cache_type = cache_type.lower()
            if self.cache_type not in ('csv', 'parquet'):
                raise ValueError(f"Invalid cache_type: {cache_type}")
        else:
            self.cache_type = CacheConfig.DEFAULT_CACHE_TYPE

        file_utils.mkdir(self.cache_directory)

    def enable(self, enabled: bool = True) -> None:
        self.enabled = enabled
        if self.enabled:
            file_utils.mkdir(self.cache_directory)
            self.save()
        elif os.path.exists(self.cache_directory):
            self.save()

    def save(self) -> None:
        data = {
            'enabled': self.enabled,
            'default_expiration': self.default_expiration,
            'cache_type': self.cache_type.lower(),  # in case of "Parquet" or "CSV", ensures a uniform filename.
        }
        file_utils.safe_jsonify(self.cache_directory, CacheConfig.CFG_FILENAME, data)


def autoload_cache(project_name: str) -> CacheConfig:
    ''' Load from the policy file if it exists, otherwise create an object. '''
    if CacheConfig.__INSTANCE__ is not None:
        return cast(CacheConfig, CacheConfig.__INSTANCE__)  # TODO: fix this+singleton annotation scheme
    cfg_handle = os.path.join(_get_env_name(project_name), CacheConfig.CFG_FILENAME)
    if os.path.isfile(cfg_handle):
        data = file_utils.load_json(cfg_handle)
        assert isinstance(data, dict)
        return CacheConfig(project_name, **data)
    return CacheConfig(project_name)


def get_cache() -> CacheConfig:
    ''' Gives the current CacheConfig object (must have been initialized elsewhere!) '''
    if CacheConfig.__INSTANCE__ is None:
        raise RuntimeError('Cache not initialized!')
    return cast(CacheConfig, CacheConfig.__INSTANCE__)
