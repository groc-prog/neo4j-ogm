# pylint: disable=missing-class-docstring


from pyneo4j_ogm.models.relationship import RelationshipModel
from pyneo4j_ogm.types.graph import EagerFetchStrategy


class TestOGMConfiguration:
    def test_default_type(self):
        class Likes(RelationshipModel):
            pass

        assert Likes.ogm_config["type"] == "LIKES"  # type: ignore

    def test_type_inheritance(self):
        class Likes(RelationshipModel):
            pass

        class Hates(Likes):
            pass

        assert Likes.ogm_config["type"] == "LIKES"  # type: ignore
        assert Hates.ogm_config["type"] == "HATES"  # type: ignore

    def test_single_pre_hook(self):
        def hook_func():
            pass

        class Likes(RelationshipModel):
            ogm_config = {"pre_hooks": {"create": hook_func}}

        assert Likes.ogm_config["pre_hooks"] == {"create": [hook_func]}  # type: ignore

    def test_multiple_pre_hook(self):
        def hook_func_one():
            pass

        def hook_func_two():
            pass

        class Likes(RelationshipModel):
            ogm_config = {"pre_hooks": {"create": [hook_func_one, hook_func_two]}}

        assert Likes.ogm_config["pre_hooks"] == {"create": [hook_func_one, hook_func_two]}  # type: ignore

    def test_single_post_hook(self):
        def hook_func():
            pass

        class Likes(RelationshipModel):
            ogm_config = {"post_hooks": {"create": hook_func}}

        assert Likes.ogm_config["post_hooks"] == {"create": [hook_func]}  # type: ignore

    def test_multiple_post_hook(self):
        def hook_func_one():
            pass

        def hook_func_two():
            pass

        class Likes(RelationshipModel):
            ogm_config = {"post_hooks": {"create": [hook_func_one, hook_func_two]}}

        assert Likes.ogm_config["post_hooks"] == {"create": [hook_func_one, hook_func_two]}  # type: ignore

    def test_primitive_config_options(self):
        class Likes(RelationshipModel):
            ogm_config = {
                "skip_constraint_creation": True,
                "skip_index_creation": True,
                "eager_fetch": True,
                "eager_fetch_strategy": EagerFetchStrategy.AS_SPLIT_QUERY,
            }

        assert Likes.ogm_config["skip_constraint_creation"] is True  # type: ignore
        assert Likes.ogm_config["skip_index_creation"] is True  # type: ignore
        assert Likes.ogm_config["eager_fetch"] is True  # type: ignore
        assert Likes.ogm_config["eager_fetch_strategy"] == EagerFetchStrategy.AS_SPLIT_QUERY  # type: ignore
