# pylint: disable=missing-class-docstring, redefined-outer-name

from os import environ

import pytest

from pyneo4j_ogm.env import EnvVariable, from_env


@pytest.fixture(autouse=True)
def reset_env_state():
    environ.clear()


class TestFromEnv:
    def test_existing_env_var(self):
        environ.update({EnvVariable.LOGGING_ENABLED.value: "0", EnvVariable.LOGLEVEL.value: "30"})

        logging_enabled = from_env(EnvVariable.LOGGING_ENABLED)
        assert logging_enabled == "0"

        loglevel = from_env(EnvVariable.LOGLEVEL)
        assert loglevel == "30"

    def test_unknown_env_var_returns_none(self):
        logging_enabled = from_env(EnvVariable.LOGGING_ENABLED)
        assert logging_enabled is None

    def test_unknown_env_var_default_value(self):
        logging_enabled = from_env(EnvVariable.LOGGING_ENABLED, 30)
        assert logging_enabled == 30
