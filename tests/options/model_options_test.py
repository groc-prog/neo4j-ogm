# pylint: disable=missing-class-docstring


import pytest

from pyneo4j_ogm.options.model_options import ModelConfigurationValidator


class TestModelConfigurationValidator:
    def test_normalize_pre_actions_normalization(self):
        with pytest.raises(ValueError):
            ModelConfigurationValidator(**{"before_actions": False})  # type: ignore

    def test_normalize_post_actions_normalization(self):
        with pytest.raises(ValueError):
            ModelConfigurationValidator(**{"after_actions": False})  # type: ignore

    def test_normalize_labels_normalization(self):
        with pytest.raises(ValueError):
            ModelConfigurationValidator(**{"labels": False})  # type: ignore
