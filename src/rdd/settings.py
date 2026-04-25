"""Kedro project settings."""

from rdd.hooks.pandera.hook import PanderaHook

HOOKS: tuple = (PanderaHook(),)

CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "config_patterns": {
        "catalog": ["**/catalog/**/*"],
        "parameters": ["**/parameters/**/*"],
        "globals": ["globals.yml"],
    },
}
