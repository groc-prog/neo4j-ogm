# pylint: disable=missing-class-docstring

from pyneo4j_ogm.models.node import NodeModel
from pyneo4j_ogm.types.graph import EagerFetchStrategy


class TestOGMConfiguration:
    def test_default_labels(self):
        class Developer(NodeModel):
            pass

        assert set(Developer.ogm_config["labels"]) == set(["Developer"])  # type: ignore

    def test_default_labels_with_multi_word_name(self):
        class DeveloperPerson(NodeModel):
            pass

        assert set(DeveloperPerson.ogm_config["labels"]) == set(["DeveloperPerson"])  # type: ignore

    def test_labels_inheritance(self):
        class Person(NodeModel):
            pass

        class Developer(Person):
            pass

        class Worker(Person):
            ogm_config = {"labels": {"HardWorking", "Human"}}

        assert set(Person.ogm_config["labels"]) == set(["Person"])  # type: ignore
        assert set(Developer.ogm_config["labels"]) == set(["Developer", "Person"])  # type: ignore
        assert set(Worker.ogm_config["labels"]) == set(["Person", "HardWorking", "Human"])  # type: ignore

    def test_labels_config(self):
        class Person(NodeModel):
            ogm_config = {"labels": "Worker"}

        assert set(Person.ogm_config["labels"]) == set(["Worker"])  # type: ignore

    def test_labels_inheritance_with_parent_config(self):
        class Person(NodeModel):
            ogm_config = {"labels": "Worker"}

        class Developer(Person):
            pass

        assert set(Person.ogm_config["labels"]) == set(["Worker"])  # type: ignore
        assert set(Developer.ogm_config["labels"]) == set(["Developer", "Worker"])  # type: ignore

    def test_labels_inheritance_with_child_config(self):
        class Person(NodeModel):
            pass

        class Developer(Person):
            ogm_config = {"labels": "PythonDeveloper"}

        assert set(Person.ogm_config["labels"]) == set(["Person"])  # type: ignore
        assert set(Developer.ogm_config["labels"]) == set(["PythonDeveloper", "Person"])  # type: ignore

    def test_labels_as_str(self):
        class Person(NodeModel):
            ogm_config = {"labels": "Worker"}

        assert set(Person.ogm_config["labels"]) == set(["Worker"])  # type: ignore

    def test_labels_as_list(self):
        class Person(NodeModel):
            ogm_config = {"labels": ["Worker", "HardWorking"]}

        assert set(Person.ogm_config["labels"]) == set(["Worker", "HardWorking"])  # type: ignore

    def test_labels_as_set(self):
        class Person(NodeModel):
            ogm_config = {"labels": {"Worker", "HardWorking"}}

        assert set(Person.ogm_config["labels"]) == set(["Worker", "HardWorking"])  # type: ignore

    def test_single_pre_hook(self):
        def hook_func():
            pass

        class Person(NodeModel):
            ogm_config = {"pre_hooks": {"create": hook_func}}

        assert Person.ogm_config["pre_hooks"] == {"create": [hook_func]}  # type: ignore

    def test_multiple_pre_hook(self):
        def hook_func_one():
            pass

        def hook_func_two():
            pass

        class Person(NodeModel):
            ogm_config = {"pre_hooks": {"create": [hook_func_one, hook_func_two]}}

        assert Person.ogm_config["pre_hooks"] == {"create": [hook_func_one, hook_func_two]}  # type: ignore

    def test_single_post_hook(self):
        def hook_func():
            pass

        class Person(NodeModel):
            ogm_config = {"post_hooks": {"create": hook_func}}

        assert Person.ogm_config["post_hooks"] == {"create": [hook_func]}  # type: ignore

    def test_multiple_post_hook(self):
        def hook_func_one():
            pass

        def hook_func_two():
            pass

        class Person(NodeModel):
            ogm_config = {"post_hooks": {"create": [hook_func_one, hook_func_two]}}

        assert Person.ogm_config["post_hooks"] == {"create": [hook_func_one, hook_func_two]}  # type: ignore

    def test_primitive_config_options(self):
        class Person(NodeModel):
            ogm_config = {
                "skip_constraint_creation": True,
                "skip_index_creation": True,
                "eager_fetch": True,
                "eager_fetch_strategy": EagerFetchStrategy.AS_SPLIT_QUERY,
            }

        assert Person.ogm_config["skip_constraint_creation"] is True  # type: ignore
        assert Person.ogm_config["skip_index_creation"] is True  # type: ignore
        assert Person.ogm_config["eager_fetch"] is True  # type: ignore
        assert Person.ogm_config["eager_fetch_strategy"] == EagerFetchStrategy.AS_SPLIT_QUERY  # type: ignore
