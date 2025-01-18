# pylint: disable=missing-class-docstring, redefined-outer-name

import json

from neo4j.spatial import CartesianPoint as Neo4jCartesianPoint
from neo4j.spatial import WGS84Point as Neo4jWGS84Point
from pydantic import BaseModel

from pyneo4j_ogm.data_types.spatial import CartesianPoint, WGS84Point


class CartesianPointModel(BaseModel):
    point: CartesianPoint


class WGS84PointModel(BaseModel):
    point: WGS84Point


class TestCartesianPoint:
    def test_parsing_from_class(self):
        model = CartesianPointModel.model_validate({"point": Neo4jCartesianPoint((1.1, 2.2, 3.3))})

        assert isinstance(model.point, Neo4jCartesianPoint)
        assert model.point.x == 1.1
        assert model.point.y == 2.2
        assert model.point.z == 3.3
        assert model.point.srid is not None

    def test_parsing_from_list(self):
        model = CartesianPointModel.model_validate({"point": [1.1, 2.2, 3.3]})

        assert isinstance(model.point, Neo4jCartesianPoint)
        assert model.point.x == 1.1
        assert model.point.y == 2.2
        assert model.point.z == 3.3
        assert model.point.srid is not None

    def test_parsing_from_tuple(self):
        model = CartesianPointModel.model_validate({"point": (1.1, 2.2, 3.3)})

        assert isinstance(model.point, Neo4jCartesianPoint)
        assert model.point.x == 1.1
        assert model.point.y == 2.2
        assert model.point.z == 3.3
        assert model.point.srid is not None

    def test_parsing_from_json(self):
        model = CartesianPointModel.model_validate_json(json.dumps({"point": [1.1, 2.2, 3.3]}))

        assert isinstance(model.point, Neo4jCartesianPoint)
        assert model.point.x == 1.1
        assert model.point.y == 2.2
        assert model.point.z == 3.3
        assert model.point.srid is not None

    # FIXME: https://github.com/pydantic/pydantic/issues/11287
    # def test_serializing(self):
    #     model = CartesianPointModel.model_validate({"point": Neo4jCartesianPoint((1.1, 2.2, 3.3))})
    #     serialized = model.model_dump()

    #     assert isinstance(serialized, dict)
    #     assert isinstance(serialized["point"], Neo4jCartesianPoint)
    #     assert serialized["point"].x == 1.1
    #     assert serialized["point"].y == 2.2
    #     assert serialized["point"].z == 3.3
    #     assert serialized["point"].srid is not None

    def test_serializing_json(self):
        model = CartesianPointModel.model_validate({"point": Neo4jCartesianPoint((1.1, 2.2, 3.3))})
        serialized = model.model_dump_json()
        parsed_serialized = json.loads(serialized)

        assert isinstance(serialized, str)
        assert isinstance(parsed_serialized["point"], list)
        assert parsed_serialized["point"] == [1.1, 2.2, 3.3]

    def test_json_schema(self):
        model = CartesianPointModel.model_validate({"point": Neo4jCartesianPoint((1.1, 2.2, 3.3))})
        schema = model.model_json_schema()

        assert "point" in schema["properties"]
        assert schema["properties"]["point"]["items"] == {"type": "number"}


class TestWGS84Point:
    def test_parsing_from_class(self):
        model = WGS84PointModel.model_validate({"point": Neo4jWGS84Point((1.1, 2.2, 3.3))})

        assert isinstance(model.point, Neo4jWGS84Point)
        assert model.point.longitude == 1.1
        assert model.point.latitude == 2.2
        assert model.point.height == 3.3
        assert model.point.srid is not None

    def test_parsing_from_list(self):
        model = WGS84PointModel.model_validate({"point": [1.1, 2.2, 3.3]})

        assert isinstance(model.point, Neo4jWGS84Point)
        assert model.point.longitude == 1.1
        assert model.point.latitude == 2.2
        assert model.point.height == 3.3
        assert model.point.srid is not None

    def test_parsing_from_tuple(self):
        model = WGS84PointModel.model_validate({"point": (1.1, 2.2, 3.3)})

        assert isinstance(model.point, Neo4jWGS84Point)
        assert model.point.longitude == 1.1
        assert model.point.latitude == 2.2
        assert model.point.height == 3.3
        assert model.point.srid is not None

    def test_parsing_from_json(self):
        model = WGS84PointModel.model_validate_json(json.dumps({"point": [1.1, 2.2, 3.3]}))

        assert isinstance(model.point, Neo4jWGS84Point)
        assert model.point.longitude == 1.1
        assert model.point.latitude == 2.2
        assert model.point.height == 3.3
        assert model.point.srid is not None

    # FIXME: https://github.com/pydantic/pydantic/issues/11287
    # def test_serializing(self):
    #     model = WGS84PointModel.model_validate({"point": Neo4jWGS84Point((1.1, 2.2, 3.3))})
    #     serialized = model.model_dump()

    #     assert isinstance(serialized, dict)
    #     assert isinstance(serialized["point"], Neo4jWGS84Point)
    #     assert serialized["point"].longitude == 1.1
    #     assert serialized["point"].latitude == 2.2
    #     assert serialized["point"].height == 3.3
    #     assert serialized["point"].srid is not None

    def test_serializing_json(self):
        model = WGS84PointModel.model_validate({"point": Neo4jWGS84Point((1.1, 2.2, 3.3))})
        serialized = model.model_dump_json()
        parsed_serialized = json.loads(serialized)

        assert isinstance(serialized, str)
        assert isinstance(parsed_serialized["point"], list)
        assert parsed_serialized["point"] == [1.1, 2.2, 3.3]

    def test_json_schema(self):
        model = WGS84PointModel.model_validate({"point": Neo4jWGS84Point((1.1, 2.2, 3.3))})
        schema = model.model_json_schema()

        assert "point" in schema["properties"]
        assert schema["properties"]["point"]["items"] == {"type": "number"}
