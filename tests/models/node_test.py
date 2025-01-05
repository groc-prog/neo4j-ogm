# pylint: disable=missing-class-docstring

from pyneo4j_ogm.models.node import NodeModel
from pyneo4j_ogm.options.model_options import ValidatedNodeConfiguration
from pyneo4j_ogm.types.graph import EagerFetchStrategy


class TestConfiguration:
    def test_default_labels(self):
        class Developer(NodeModel):
            pass

        assert set(Developer._ogm_config.labels) == set(["Developer"])  # type: ignore

    def test_default_labels_with_multi_word_name(self):
        class DeveloperPerson(NodeModel):
            pass

        assert set(DeveloperPerson._ogm_config.labels) == set(["DeveloperPerson"])  # type: ignore

    def test_labels_inheritance(self):
        class Person(NodeModel):
            pass

        class Developer(Person):
            pass

        class Worker(Person):
            ogm_config = {"labels": {"HardWorking", "Human"}}

        assert set(Person._ogm_config.labels) == set(["Person"])  # type: ignore
        assert set(Developer._ogm_config.labels) == set(["Developer", "Person"])  # type: ignore
        assert set(Worker._ogm_config.labels) == set(["Person", "HardWorking", "Human"])  # type: ignore

    def test_labels_config(self):
        class Person(NodeModel):
            ogm_config = {"labels": "Worker"}

        assert set(Person._ogm_config.labels) == set(["Worker"])  # type: ignore

    def test_labels_inheritance_with_parent_config(self):
        class Person(NodeModel):
            ogm_config = {"labels": "Worker"}

        class Developer(Person):
            pass

        assert set(Person._ogm_config.labels) == set(["Worker"])  # type: ignore
        assert set(Developer._ogm_config.labels) == set(["Developer", "Worker"])  # type: ignore

    def test_labels_inheritance_with_child_config(self):
        class Person(NodeModel):
            pass

        class Developer(Person):
            ogm_config = {"labels": "PythonDeveloper"}

        assert set(Person._ogm_config.labels) == set(["Person"])  # type: ignore
        assert set(Developer._ogm_config.labels) == set(["PythonDeveloper", "Person"])  # type: ignore

    def test_labels_as_str(self):
        class Person(NodeModel):
            ogm_config = {"labels": "Worker"}

        assert set(Person._ogm_config.labels) == set(["Worker"])  # type: ignore

    def test_labels_as_list(self):
        class Person(NodeModel):
            ogm_config = {"labels": ["Worker", "HardWorking"]}

        assert set(Person._ogm_config.labels) == set(["Worker", "HardWorking"])  # type: ignore

    def test_labels_as_set(self):
        class Person(NodeModel):
            ogm_config = {"labels": {"Worker", "HardWorking"}}

        assert set(Person._ogm_config.labels) == set(["Worker", "HardWorking"])  # type: ignore

    def test_single_pre_hook(self):
        def hook_func():
            pass

        class Person(NodeModel):
            ogm_config = {"pre_hooks": {"create": hook_func}}

        assert Person._ogm_config.pre_hooks == {"create": [hook_func]}  # type: ignore

    def test_multiple_pre_hook(self):
        def hook_func_one():
            pass

        def hook_func_two():
            pass

        class Person(NodeModel):
            ogm_config = {"pre_hooks": {"create": [hook_func_one, hook_func_two]}}

        assert Person._ogm_config.pre_hooks == {"create": [hook_func_one, hook_func_two]}  # type: ignore

    def test_single_post_hook(self):
        def hook_func():
            pass

        class Person(NodeModel):
            ogm_config = {"post_hooks": {"create": hook_func}}

        assert Person._ogm_config.post_hooks == {"create": [hook_func]}  # type: ignore

    def test_multiple_post_hook(self):
        def hook_func_one():
            pass

        def hook_func_two():
            pass

        class Person(NodeModel):
            ogm_config = {"post_hooks": {"create": [hook_func_one, hook_func_two]}}

        assert Person._ogm_config.post_hooks == {"create": [hook_func_one, hook_func_two]}  # type: ignore

    def test_primitive_config_options(self):
        class Person(NodeModel):
            ogm_config = {
                "skip_constraint_creation": True,
                "skip_index_creation": True,
                "eager_fetch": True,
                "eager_fetch_strategy": EagerFetchStrategy.AS_SPLIT_QUERY,
            }

        assert Person._ogm_config.skip_constraint_creation is True  # type: ignore
        assert Person._ogm_config.skip_index_creation is True  # type: ignore
        assert Person._ogm_config.eager_fetch is True  # type: ignore
        assert Person._ogm_config.eager_fetch_strategy == EagerFetchStrategy.AS_SPLIT_QUERY  # type: ignore


class TestValidatedConfiguration:
    def test_default_node_config(self):
        class Developer(NodeModel):
            pass

        configuration = Developer.pyneo4j_config()

        assert isinstance(configuration, ValidatedNodeConfiguration)
        assert configuration.pre_hooks == {}
        assert configuration.post_hooks == {}
        assert not configuration.skip_constraint_creation
        assert not configuration.skip_index_creation
        assert not configuration.eager_fetch
        assert configuration.eager_fetch_strategy == EagerFetchStrategy.COMBINED
        assert configuration.labels == ["Developer"]

    def test_custom_node_config(self):
        def hook():
            pass

        class Developer(NodeModel):
            ogm_config = {
                "labels": ["Human", "Genius"],
                "pre_hooks": {"create": hook},
                "post_hooks": {"create": [hook]},
                "skip_constraint_creation": True,
                "skip_index_creation": True,
                "eager_fetch": True,
                "eager_fetch_strategy": EagerFetchStrategy.AS_SPLIT_QUERY,
            }

        configuration = Developer.pyneo4j_config()

        assert isinstance(configuration, ValidatedNodeConfiguration)
        assert configuration.pre_hooks == {"create": [hook]}
        assert configuration.post_hooks == {"create": [hook]}
        assert configuration.skip_constraint_creation
        assert configuration.skip_index_creation
        assert configuration.eager_fetch
        assert configuration.eager_fetch_strategy == EagerFetchStrategy.AS_SPLIT_QUERY
        assert configuration.labels == ["Human", "Genius"]
