import json
import orjson
import os
import time
# import uuid

# from copy import deepcopy
from datetime import datetime, timedelta, timezone

# from random import randint
from urllib.parse import parse_qs, urlparse, urlsplit

import pytest
import pystac
from pydantic.datetime_parse import parse_datetime
from shapely.geometry import shape

# from shapely.geometry import Polygon
from stac_fastapi.types.core import LandingPageMixin
from stac_fastapi.types.rfc3339 import datetime_to_str, rfc3339_str_to_datetime

from stac_fastapi.sqlalchemy.core import CoreCrudClient


@pytest.mark.skip(reason="Database is readonly")
def test_create_and_delete_item(app_client, load_test_data):
    """Test creation and deletion of a single item (transactions extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    resp = app_client.delete(
        f"/collections/{test_item['collection']}/items/{resp.json()['id']}"
    )
    assert resp.status_code == 200


@pytest.mark.skip(reason="Database is readonly")
def test_create_item_conflict(app_client, load_test_data):
    """Test creation of an item which already exists (transactions extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 409


@pytest.mark.skip(reason="Database is readonly")
def test_create_item_duplicate(app_client, load_test_data):
    """Test creation of an item id which already exists but in a different collection(transactions extension)"""

    # add test_item to test-collection
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # add test_item to test-collection again, resource already exists
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 409

    # create "test-collection-2"
    collection_2 = load_test_data("test_collection.json")
    collection_2["id"] = "test-collection-2"
    resp = app_client.post("/collections", json=collection_2)
    assert resp.status_code == 200

    # add test_item to test-collection-2, posts successfully
    test_item["collection"] = "test-collection-2"
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200


@pytest.mark.skip(reason="Database is readonly")
def test_delete_item_duplicate(app_client, load_test_data):
    """Test creation of an item id which already exists but in a different collection(transactions extension)"""

    # add test_item to test-collection
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # create "test-collection-2"
    collection_2 = load_test_data("test_collection.json")
    collection_2["id"] = "test-collection-2"
    resp = app_client.post("/collections", json=collection_2)
    assert resp.status_code == 200

    # add test_item to test-collection-2
    test_item["collection"] = "test-collection-2"
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # delete test_item from test-collection
    test_item["collection"] = "test-collection"
    resp = app_client.delete(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert resp.status_code == 200

    # test-item in test-collection has already been deleted
    resp = app_client.delete(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert resp.status_code == 404

    # test-item in test-collection-2 still exists, was not deleted
    test_item["collection"] = "test-collection-2"
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 409


@pytest.mark.skip(reason="Database is readonly")
def test_update_item_duplicate(app_client, load_test_data):
    """Test creation of an item id which already exists but in a different collection(transactions extension)"""

    # add test_item to test-collection
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # create "test-collection-2"
    collection_2 = load_test_data("test_collection.json")
    collection_2["id"] = "test-collection-2"
    resp = app_client.post("/collections", json=collection_2)
    assert resp.status_code == 200

    # add test_item to test-collection-2
    test_item["collection"] = "test-collection-2"
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # update gsd in test_item, test-collection-2
    test_item["properties"]["gsd"] = 16
    resp = app_client.put(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        json=test_item,
    )
    assert resp.status_code == 200
    updated_item = resp.json()
    assert updated_item["properties"]["gsd"] == 16

    # update gsd in test_item, test-collection
    test_item["collection"] = "test-collection"
    test_item["properties"]["gsd"] = 17
    resp = app_client.put(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        json=test_item,
    )
    assert resp.status_code == 200
    updated_item = resp.json()
    assert updated_item["properties"]["gsd"] == 17

    # test_item in test-collection, updated gsd = 17
    resp = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert resp.status_code == 200
    item = resp.json()
    assert item["properties"]["gsd"] == 17

    # test_item in test-collection-2, updated gsd = 16
    test_item["collection"] = "test-collection-2"
    resp = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert resp.status_code == 200
    item = resp.json()
    assert item["properties"]["gsd"] == 16


@pytest.mark.skip(reason="Database is readonly")
def test_delete_missing_item(app_client, load_test_data):
    """Test deletion of an item which does not exist (transactions extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.delete(f"/collections/{test_item['collection']}/items/hijosh")
    assert resp.status_code == 404


@pytest.mark.skip(reason="Database is readonly")
def test_create_item_missing_collection(app_client, load_test_data):
    """Test creation of an item without a parent collection (transactions extension)"""
    test_item = load_test_data("test_item.json")
    test_item["collection"] = "stac is cool"
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 424


@pytest.mark.skip(reason="Database is readonly")
def test_update_item_already_exists(app_client, load_test_data):
    """Test updating an item which already exists (transactions extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    assert test_item["properties"]["gsd"] != 16
    test_item["properties"]["gsd"] = 16
    resp = app_client.put(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        json=test_item,
    )
    updated_item = resp.json()
    assert updated_item["properties"]["gsd"] == 16


@pytest.mark.skip(reason="Database is readonly")
def test_update_new_item(app_client, load_test_data):
    """Test updating an item which does not exist (transactions extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.put(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        json=test_item,
    )
    assert resp.status_code == 404


@pytest.mark.skip(reason="Database is readonly")
def test_update_item_missing_collection(app_client, load_test_data):
    """Test updating an item without a parent collection (transactions extension)"""
    test_item = load_test_data("test_item.json")

    # Create the item
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # Try to update collection of the item
    test_item["collection"] = "stac is cool"
    resp = app_client.put(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        json=test_item,
    )
    assert resp.status_code == 404


@pytest.mark.skip(reason="Database is readonly")
def test_update_item_geometry(app_client, load_test_data):
    test_item = load_test_data("test_item.json")

    # Create the item
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # Update the geometry of the item
    test_item["geometry"]["coordinates"] = [[[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]]
    resp = app_client.put(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        json=test_item,
    )
    assert resp.status_code == 200

    # Fetch the updated item
    resp = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert resp.status_code == 200
    assert resp.json()["geometry"]["coordinates"] == [
        [[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]
    ]


def test_get_item(app_client, load_test_data):
    """Test read an item by id (core)"""
    test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    get_item = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert get_item.status_code == 200
    resp_json = get_item.json()
    assert resp_json["id"] == test_item["id"]


@pytest.mark.skip(
    reason="Validation fails because we allow unknown query parameters due to technical limitations"
)
def test_returns_valid_item(app_client, load_test_data):
    """Test validates fetched item with jsonschema"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    get_item = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}"
    )
    assert get_item.status_code == 200
    item_dict = get_item.json()
    # Mock root to allow validation
    mock_root = pystac.Catalog(
        id="test", description="test desc", href="https://example.com"
    )
    item = pystac.Item.from_dict(item_dict, preserve_dict=False, root=mock_root)
    item.validate()


def test_get_item_collection(app_client, load_test_data):
    """Test read an item collection (core)"""
    item_count = 1467880
    test_item = load_test_data("test_item.json")

    # for idx in range(item_count):
    #     _test_item = deepcopy(test_item)
    #     _test_item["id"] = test_item["id"] + str(idx)
    #     resp = app_client.post(
    #         f"/collections/{test_item['collection']}/items", json=_test_item
    #     )
    #     assert resp.status_code == 200

    resp = app_client.get(f"/collections/{test_item['collection']}/items")
    assert resp.status_code == 200

    item_collection = resp.json()
    assert item_collection["context"]["matched"] >= item_count


def test_pagination(app_client, load_test_data):
    """Test item collection pagination (paging extension)"""
    # item_count = 10
    test_item = load_test_data("test_item.json")

    # for idx in range(item_count):
    #     _test_item = deepcopy(test_item)
    #     _test_item["id"] = test_item["id"] + str(idx)
    #     resp = app_client.post(
    #         f"/collections/{test_item['collection']}/items", json=_test_item
    #     )
    #     assert resp.status_code == 200

    resp = app_client.get(
        f"/collections/{test_item['collection']}/items", params={"limit": 3}
    )
    assert resp.status_code == 200
    first_page = resp.json()
    assert first_page["context"]["returned"] == 3

    url_components = urlsplit(first_page["links"][0]["href"])
    resp = app_client.get(f"{url_components.path}?{url_components.query}")
    assert resp.status_code == 200
    second_page = resp.json()
    assert second_page["context"]["returned"] == 3


@pytest.mark.skip(reason="Database is readonly")
def test_item_timestamps(app_client, load_test_data):
    """Test created and updated timestamps (common metadata)"""
    test_item = load_test_data("test_item.json")
    start_time = datetime.now(timezone.utc)
    time.sleep(2)
    # Confirm `created` timestamp
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    item = resp.json()
    created_dt = parse_datetime(item["properties"]["created"])
    assert resp.status_code == 200
    assert start_time < created_dt < datetime.now(timezone.utc)

    time.sleep(2)
    # Confirm `updated` timestamp
    item["properties"]["proj:epsg"] = 4326
    resp = app_client.put(
        f"/collections/{test_item['collection']}/items/{item['id']}", json=item
    )
    assert resp.status_code == 200
    updated_item = resp.json()

    # Created shouldn't change on update
    assert item["properties"]["created"] == updated_item["properties"]["created"]
    assert parse_datetime(updated_item["properties"]["updated"]) > created_dt


def test_item_search_by_id_post(app_client, load_test_data):
    """Test POST search by item id (core)"""
    ids = [
        "2021_85_47_5_0032_00000135",
        "2021_84_41_4_0019_00105137",
        "2021_84_41_3_0019_00105137",
    ]
    # for id in ids:
    #     test_item = load_test_data("test_item.json")
    #     test_item["id"] = id
    #     resp = app_client.post(
    #         f"/collections/{test_item['collection']}/items", json=test_item
    #     )
    #     assert resp.status_code == 200

    params = {"collections": ["skraafotos2021"], "ids": ids}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == len(ids)
    assert set([feat["id"] for feat in resp_json["features"]]) == set(ids)


def test_item_search_spatial_query_post(app_client, load_test_data):
    """Test POST search with spatial query (core)"""
    test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]


def test_item_search_temporal_query_post(app_client, load_test_data):
    """Test POST search with single-tailed spatio-temporal query (core)"""
    test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])
    item_date = item_date + timedelta(seconds=1)

    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
        "datetime": f"../{datetime_to_str(item_date)}",
    }
    resp = app_client.post("/search", json=params)
    resp_json = resp.json()
    assert resp.status_code == 200
    assert resp_json["features"][0]["id"] == test_item["id"]


def test_item_search_temporal_window_post(app_client, load_test_data):
    """Test POST search with two-tailed spatio-temporal query (core)"""
    test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])
    item_date_before = item_date - timedelta(seconds=1)
    item_date_after = item_date + timedelta(seconds=1)

    params = {
        "collections": [test_item["collection"]],
        "intersects": test_item["geometry"],
        "datetime": f"{datetime_to_str(item_date_before)}/{datetime_to_str(item_date_after)}",
    }
    resp = app_client.post("/search", json=params)
    resp_json = resp.json()
    assert resp.status_code == 200
    assert resp_json["features"][0]["id"] == test_item["id"]


def test_item_search_temporal_open_window(app_client, load_test_data):
    """Test POST search with open spatio-temporal query (core)"""
    # test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    for dt in ["/", "../", "/..", "../.."]:
        resp = app_client.post("/search", json={"datetime": dt})
        assert resp.status_code == 400
        resp_json = resp.json()
        assert resp_json["detail"] == "Double open-ended intervals are not allowed."

    for dt in ["/", "../", "/..", "../.."]:
        resp = app_client.get("/search", params={"datetime": dt})
        assert resp.status_code == 400
        resp_json = resp.json()
        assert resp_json["detail"] == "Double open-ended intervals are not allowed."


def test_item_search_sort_post(app_client, load_test_data):
    """Test POST search with sorting (sort extension)"""
    first_item = load_test_data("test_item.json")
    item_date = rfc3339_str_to_datetime(first_item["properties"]["datetime"])
    # resp = app_client.post(
    #     f"/collections/{first_item['collection']}/items", json=first_item
    # )
    # assert resp.status_code == 200

    second_item = load_test_data("test_item.json")
    second_item["id"] = "2021_82_20_2_0001_00005355"
    another_item_date = item_date - timedelta(days=1)
    second_item["properties"]["datetime"] = datetime_to_str(another_item_date)
    # resp = app_client.post(
    #     f"/collections/{second_item['collection']}/items", json=second_item
    # )
    # assert resp.status_code == 200

    params = {
        "collections": [first_item["collection"]],
        "sortby": [{"field": "datetime", "direction": "desc"}],
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


def test_item_search_by_id_get(app_client, load_test_data):
    """Test GET search by item id (core)"""
    ids = [
        "2021_82_20_1_0001_00005346",
        "2021_82_20_1_0001_00005347",
        "2021_82_20_1_0001_00005348",
    ]
    # for id in ids:
    test_item = load_test_data("test_item.json")
    #     test_item["id"] = id
    #     resp = app_client.post(
    #         f"/collections/{test_item['collection']}/items", json=test_item
    #     )
    #     assert resp.status_code == 200

    params = {"collections": test_item["collection"], "ids": ",".join(ids)}
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == len(ids)
    assert set([feat["id"] for feat in resp_json["features"]]) == set(ids)


def test_item_search_bbox_get(app_client, load_test_data):
    """Test GET search with spatial query (core)"""
    test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    params = {
        "collections": test_item["collection"],
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
    }
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]


def test_item_search_get_without_collections(app_client, load_test_data):
    """Test GET search without specifying collections"""
    test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    params = {
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
    }
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]
    assert len(resp_json["features"]) >= 1


def test_item_search_temporal_window_get(app_client, load_test_data):
    """Test GET search with spatio-temporal query (core)"""
    test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    item_date = rfc3339_str_to_datetime(test_item["properties"]["datetime"])
    # item_date_before = item_date - timedelta(seconds=1)
    # item_date_after = item_date + timedelta(seconds=1)
    item_date_before = item_date - timedelta(seconds=20)
    item_date_after = item_date + timedelta(seconds=10)

    params = {
        "collections": test_item["collection"],
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
        "datetime": f"{datetime_to_str(item_date_before)}/{datetime_to_str(item_date_after)}",
    }
    resp = app_client.get("/search", params=params)
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]

    assert any(
        test_item["id"] == f["id"] for f in resp_json["features"]
    ), "test item should be returned within interval"

    assert all(
        datetime_to_str(item_date_after) >= f["properties"]["datetime"]
        for f in resp_json["features"]
    ), "Item with datetime outside (greater than) filter interval"

    assert all(
        datetime_to_str(item_date_before) <= f["properties"]["datetime"]
        for f in resp_json["features"]
    ), "Item with datetime outside (less than) filter interval"


def test_item_search_sort_get(app_client, load_test_data):
    """Test GET search with sorting (sort extension)"""
    first_item = load_test_data("test_item.json")
    item_date = rfc3339_str_to_datetime(first_item["properties"]["datetime"])
    # resp = app_client.post(
    #     f"/collections/{first_item['collection']}/items", json=first_item
    # )
    # assert resp.status_code == 200

    second_item = load_test_data("test_item.json")
    second_item["id"] = "2021_82_20_2_0001_00005355"
    another_item_date = item_date - timedelta(days=1)
    second_item["properties"]["datetime"] = datetime_to_str(another_item_date)
    # resp = app_client.post(
    #     f"/collections/{second_item['collection']}/items", json=second_item
    # )
    # assert resp.status_code == 200
    params = {"collections": [first_item["collection"]], "sortby": "-datetime"}
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][1]["id"] == second_item["id"]


def test_item_search_sort_get_no_prefix(app_client, load_test_data):
    """Test GET search with sorting with no default prefix(sort extension)"""
    first_item = load_test_data("test_item.json")

    params = {"collections": [first_item["collection"]], "sortby": "datetime"}
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == "2021_85_45_1_0045_00000003"
    assert resp_json["features"][1]["id"] == "2021_85_45_2_0045_00000003"


def test_item_search_sort_datetime_asc_id_desc_get(app_client, load_test_data):
    """Test GET search with sorting (sort extension)"""
    first_item = load_test_data("test_item.json")

    params = {"collections": [first_item["collection"]], "sortby": "+datetime,-id"}
    resp = app_client.get("/search", params=params)

    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10

    id1 = resp_json["features"][0]["id"]
    id10 = resp_json["features"][9]["id"]
    assert id1 > id10

    date1 = resp_json["features"][0]["properties"]["datetime"]
    date10 = resp_json["features"][9]["properties"]["datetime"]
    assert date1 < date10


def test_item_search_post_without_collection(app_client, load_test_data):
    """Test POST search without specifying a collection"""
    test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    params = {
        "bbox": test_item["bbox"],
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


@pytest.mark.skip(reason="proj:epsg is null")
def test_item_search_properties_jsonb(app_client, load_test_data):
    """Test POST search with JSONB query (query extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # EPSG is a JSONB key
    params = {"query": {"proj:epsg": {"gt": test_item["properties"]["proj:epsg"] + 1}}}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


# def test_item_search_properties_field(app_client, load_test_data):
def test_item_search_properties_field(app_client):
    """Test POST search indexed field with query (query extension)"""
    # test_item = load_test_data("test_item.json")
    # resp = app_client.post(
    #     f"/collections/{test_item['collection']}/items", json=test_item
    # )
    # assert resp.status_code == 200

    # Orientation is an indexed field
    # params = {"query": {"orientation": {"eq": "south"}}}
    params = {"filter-lang": "cql-json", "filter": {"eq": [{"property": "gsd"}, "0.075"]}}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10
    for response in resp_json["features"]:
        assert response["properties"]["gsd"] == 0.075

    params = {
        "filter-lang": "cql-json",
        "filter": {"eq": [{"property": "direction"}, "south"]},
    }

    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10
    for response in resp_json["features"]:
        assert response["properties"]["direction"] == "south"


@pytest.mark.skip(reason="Query Extension switched off")
def test_item_search_get_query_extension(app_client, load_test_data):
    """Test GET search with JSONB query (query extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    # EPSG is a JSONB key
    params = {
        "collections": [test_item["collection"]],
        "query": json.dumps(
            {"proj:epsg": {"gt": test_item["properties"]["proj:epsg"] + 1}}
        ),
    }
    resp = app_client.get("/search", params=params)
    assert resp.json()["context"]["returned"] == 0

    params["query"] = json.dumps(
        {"proj:epsg": {"eq": test_item["properties"]["proj:epsg"]}}
    )
    resp = app_client.get("/search", params=params)
    resp_json = resp.json()
    assert resp_json["context"]["returned"] == 1
    assert (
        resp_json["features"][0]["properties"]["proj:epsg"]
        == test_item["properties"]["proj:epsg"]
    )


def test_item_search_get_filter_extension(app_client, load_test_data):
    """Test GET search with JSONB query (query extension)"""
    test_item = load_test_data("test_item.json")

    params = {
        "collections": [test_item["collection"]],
        "filter-lang": "cql-json",
        "filter": json.dumps(
            {
                "gt": [
                    {"property": "gsd"},
                    test_item["properties"]["gsd"] + 1,
                ]
            }
        ),
    }
    resp = app_client.get("/search", params=params)
    assert resp.json()["context"]["returned"] == 0

    params["filter"] = json.dumps(
        {"eq": [{"property": "gsd"}, test_item["properties"]["gsd"]]}
    )
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["context"]["returned"] >= 2
    assert resp_json["features"][0]["properties"]["gsd"] == test_item["properties"]["gsd"]


def test_item_search_pagination(app_client, load_test_data):
    """Test format of pagination links on a GET search"""
    # test_item = load_test_data("test_item.json")
    # for x in range(20):
    #     test_item["id"] = f"test_item_{x}"
    #     resp = app_client.post(
    #         f"/collections/{test_item['collection']}/items", json=test_item
    #     )
    #     assert resp.status_code == 200

    params = {"limit": 5}
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200

    resp_json = resp.json()
    links = resp_json["links"]
    next_link = next(link for link in links if link["rel"] == "next")
    assert next_link["href"].startswith("http://testserver/search?")

    # resp = app_client.get(links[0]["href"])
    resp = app_client.get(links[1]["href"])
    resp_json = resp.json()
    links = resp_json["links"]
    next_link = next(link for link in links if link["rel"] == "next")
    prev_link = next(link for link in links if link["rel"] == "previous")
    assert next_link["href"].startswith("http://testserver/search?")
    assert prev_link["href"].startswith("http://testserver/search?")


def test_get_missing_item_collection(app_client):
    """Test reading a collection which does not exist"""
    resp = app_client.get("/collections/invalid-collection/items")
    assert resp.status_code == 404
    resp_json = resp.json()
    assert resp_json["code"] == "NotFoundError"
    assert resp_json["description"] == "Collection invalid-collection not found"


def test_pagination_item_collection(app_client, load_test_data):
    """Test item collection pagination links (paging extension)"""
    test_item = load_test_data("test_item.json")
    # ids = []
    ids = [
        "2021_82_20_1_0001_00005355",
        "2021_82_20_2_0001_00005355",
        "2021_82_20_3_0001_00005355",
        "2021_82_20_4_0001_00005355",
        "2021_82_20_5_0001_00005355",
    ]

    # Ingest 5 items
    # for idx in range(5):
    #     uid = str(uuid.uuid4())
    #     test_item["id"] = uid
    #     resp = app_client.post(
    #         f"/collections/{test_item['collection']}/items", json=test_item
    #     )
    #     assert resp.status_code == 200
    #     ids.append(uid)

    # Paginate through all 5 items with a limit of 1 (expecting 5 requests)
    # page = app_client.get(
    #     f"/collections/{test_item['collection']}/items", params={"limit": 1}
    # )
    page = app_client.get(
        f"/collections/{test_item['collection']}/items",
        params={"limit": 1, "datetime": "2021-11-22T10:03:41Z"},
    )
    idx = 0
    item_ids = []
    while True:
        idx += 1
        page_data = page.json()
        item_ids.append(page_data["features"][0]["id"])
        next_link = list(filter(lambda link: link["rel"] == "next", page_data["links"]))
        if not next_link:
            break
        query_params = parse_qs(urlparse(next_link[0]["href"]).query)
        page = app_client.get(
            f"/collections/{test_item['collection']}/items",
            params=query_params,
        )
        # Break here to avoud having an infinite loop in a test case
        if idx > len(ids) + 2:
            break

    # Our limit is 1 so we expect len(ids) number of requests before we run out of pages
    assert idx == len(ids)

    # Confirm we have paginated through all items
    assert not set(item_ids) - set(ids)


def test_pagination_post(app_client, load_test_data):
    """Test POST pagination (paging extension)"""
    # test_item = load_test_data("test_item.json")
    # ids = []
    ids = [
        "2017_82_20_4_2031_00030536",
        "2017_82_19_1_0039_00160059",
        "2017_82_19_2_0039_00160059",
    ]

    # Ingest 5 items
    # for idx in range(5):
    #     uid = str(uuid.uuid4())
    #     test_item["id"] = uid
    #     resp = app_client.post(
    #         f"/collections/{test_item['collection']}/items", json=test_item
    #     )
    #     assert resp.status_code == 200
    #     ids.append(uid)

    # Paginate through all 5 items with a limit of 1 (expecting 5 requests)
    request_body = {"ids": ids, "limit": 1}
    page = app_client.post("/search", json=request_body)
    idx = 0
    item_ids = []
    while True:
        idx += 1
        page_data = page.json()
        item_ids.append(page_data["features"][0]["id"])
        next_link = list(filter(lambda link: link["rel"] == "next", page_data["links"]))
        if not next_link:
            break
        # Merge request bodies
        request_body.update(next_link[0]["body"])
        page = app_client.post("/search", json=request_body)

    # Our limit is 1 so we expect len(ids) number of requests before we run out of pages
    assert idx == len(ids)

    # Confirm we have paginated through all items
    assert not set(item_ids) - set(ids)


# def test_pagination_token_idempotent(app_client, load_test_data):
def test_pagination_token_idempotent(app_client):
    """Test that pagination tokens are idempotent (paging extension)"""
    # test_item = load_test_data("test_item.json")
    # ids = []
    ids = [
        "2017_82_20_4_2031_00030536",
        "2017_82_19_1_0039_00160059",
        "2017_82_19_2_0039_00160059",
    ]

    # Ingest 5 items
    # for idx in range(5):
    #     uid = str(uuid.uuid4())
    #     test_item["id"] = uid
    #     resp = app_client.post(
    #         f"/collections/{test_item['collection']}/items", json=test_item
    #     )
    #     assert resp.status_code == 200
    #     ids.append(uid)

    # page = app_client.get("/search", params={"ids": ",".join(ids), "limit": 3})
    page = app_client.get("/search", params={"ids": ",".join(ids), "limit": 1})
    page_data = page.json()
    next_link = list(filter(lambda link: link["rel"] == "next", page_data["links"]))

    assert page_data["links"] != []
    # Confirm token is idempotent
    resp1 = app_client.get(
        "/search", params=parse_qs(urlparse(next_link[0]["href"]).query)
    )
    resp2 = app_client.get(
        "/search", params=parse_qs(urlparse(next_link[0]["href"]).query)
    )
    resp1_data = resp1.json()
    resp2_data = resp2.json()

    # Two different requests with the same pagination token should return the same items
    assert [item["id"] for item in resp1_data["features"]] == [
        item["id"] for item in resp2_data["features"]
    ]


@pytest.mark.skip(reason="Field extension switched off")
def test_field_extension_get(app_client, load_test_data):
    """Test GET search with included fields (fields extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    params = {"fields": "+properties.proj:epsg,+properties.gsd"}
    resp = app_client.get("/search", params=params)
    feat_properties = resp.json()["features"][0]["properties"]
    assert not set(feat_properties) - {"proj:epsg", "gsd", "datetime"}


@pytest.mark.skip(reason="Field extension switched off")
def test_field_extension_post(app_client, load_test_data):
    """Test POST search with included and excluded fields (fields extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    body = {
        "fields": {
            "exclude": ["assets.B1"],
            "include": ["properties.eo:cloud_cover", "properties.orientation"],
        }
    }

    resp = app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "B1" not in resp_json["features"][0]["assets"].keys()
    assert not set(resp_json["features"][0]["properties"]) - {
        "orientation",
        "eo:cloud_cover",
        "datetime",
    }


@pytest.mark.skip(reason="Field extension switched off")
def test_field_extension_exclude_and_include(app_client, load_test_data):
    """Test POST search including/excluding same field (fields extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    body = {
        "fields": {
            "exclude": ["properties.eo:cloud_cover"],
            "include": ["properties.eo:cloud_cover"],
        }
    }

    resp = app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "eo:cloud_cover" not in resp_json["features"][0]["properties"]


@pytest.mark.skip(reason="Field extension switched off")
def test_field_extension_exclude_default_includes(app_client, load_test_data):
    """Test POST search excluding a forbidden field (fields extension)"""
    test_item = load_test_data("test_item.json")
    resp = app_client.post(
        f"/collections/{test_item['collection']}/items", json=test_item
    )
    assert resp.status_code == 200

    body = {"fields": {"exclude": ["geometry"]}}

    resp = app_client.post("/search", json=body)
    resp_json = resp.json()
    assert "geometry" not in resp_json["features"][0]


# def test_search_intersects_and_bbox(app_client):
def test_search_intersects_and_bbox(load_test_data, app_client):
    """Test POST search intersects and bbox are mutually exclusive (core)"""
    item = load_test_data("test_item.json")

    point = [8.4570, 56.24298]
    intersects = {"type": "Point", "coordinates": point}
    bbox = [8.4570, 56.24298, 8.5570, 56.34298]

    params = {
        "intersects": intersects,
        "collections": [item["collection"]],
        "bbox": bbox,
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert (
        "[{'loc': ('body', 'intersects'), 'msg': 'intersects and bbox parameters are mutually exclusive', 'type': 'value_error'}]"
        in resp_json["description"]
    )

    get_params = {
        "intersects": orjson.dumps(params["intersects"]).decode("utf-8"),
        "collections": item["collection"],
        "bbox": "8.4570, 56.24298, 8.5570, 56.34298",
    }

    resp = app_client.get("/search", params=get_params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert (
        "1 validation error for SearchPostRequest\nintersects\n  intersects and bbox parameters are mutually exclusive (type=value_error)"
        in resp_json["description"]
    )


def test_get_missing_item(app_client, load_test_data):
    """Test read item which does not exist (transactions extension)"""
    test_coll = load_test_data("test_collection.json")
    resp = app_client.get(f"/collections/{test_coll['id']}/items/invalid-item")
    assert resp.status_code == 404
    resp_json = resp.json()
    assert resp_json["code"] == "NotFoundError"
    assert resp_json["description"] == "Item invalid-item not found"


@pytest.mark.skip(reason="Query extension switched off")
def test_search_invalid_query_field(app_client):
    body = {"query": {"gsd": {"lt": 100}, "invalid-field": {"eq": 50}}}
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 400


def test_search_invalid_filter_field(app_client):
    body = {
        "filter-lang": "cql-json",
        "filter": {"eq": [{"property": "invalid-field"}, 50]},
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert resp_json["description"] == "Cannot search on field: invalid-field"


def test_item_search_cql_and(app_client, load_test_data):
    test_item = load_test_data("test_item.json")

    body = {
        "filter-lang": "cql-json",
        "filter": {
            "and": [
                {"eq": [{"property": "gsd"}, test_item["properties"]["gsd"]]},
                {
                    "eq": [
                        {"property": "datetime"},
                        test_item["properties"]["datetime"],
                    ]
                },
            ]
        },
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["context"]["returned"] == 5


def test_item_search_cql_or(app_client, load_test_data):
    test_item = load_test_data("test_item.json")

    body = {
        "filter-lang": "cql-json",
        "filter": {
            "or": [
                {"eq": [{"property": "gsd"}, test_item["properties"]["gsd"]]},
                {
                    "eq": [
                        {"property": "datetime"},
                        test_item["properties"]["datetime"] + "1",
                    ]
                },
            ]
        },
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["context"]["returned"] == 10


def test_item_search_cql_not(app_client):
    body = {
        "filter-lang": "cql-json",
        "filter": {"not": {"lt": [{"property": "gsd"}, 100]}},
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["context"]["returned"] == 0


def test_item_search_cql_isNull(app_client):
    body = {"filter-lang": "cql-json", "filter": {"isNull": {"property": "id"}}}
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["context"]["returned"] == 0


def test_item_search_cql_between(app_client, load_test_data):
    test_item = load_test_data("test_item.json")

    body = {
        "filter-lang": "cql-json",
        "filter": {
            "between": {
                "value": {"property": "gsd"},
                "lower": test_item["properties"]["gsd"] - 1,
                "upper": test_item["properties"]["gsd"] + 1,
            }
        },
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["context"]["returned"] == 10


def test_item_search_cql_invalid_operation(app_client):
    body = {
        "filter-lang": "cql-json",
        "filter": {"invalid_op": [{"property": "gsd"}, 1]},
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "Unable to parse expression node {'invalid_op': [{'property': 'gsd'}, 1]}"
    )


def test_item_search_invalid_filter_lang(app_client):
    body = {
        "filter-lang": "invalid-lang",
        "filter": {"eq": [{"property": "gsd"}, 1]},
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 400

    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "[{'loc': ('body', 'filter-lang'), 'msg': \"value is not a valid enumeration member; permitted: 'cql-json'\", 'type': 'type_error.enum', 'ctx': {'enum_values': [<FilterLang.cql_json: 'cql-json'>]}}]"
    )


def test_item_search_invalid_filter_crs(app_client):
    body = {
        "filter-crs": "invalid-crs",
        "filter": {"eq": [{"property": "gsd"}, 1]},
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 400

    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "[{'loc': ('body', 'filter-crs'), 'msg': \"unexpected value; permitted: 'http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832'\", 'type': 'value_error.const', 'ctx': {'given': 'invalid-crs', 'permitted': ('http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832')}}]"
    )


def test_search_bbox_errors(app_client):
    # body = {"query": {"bbox": [0]}}
    body = {"bbox": [0]}
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "[{'loc': ('body', 'bbox'), 'msg': 'not enough values to unpack (expected 6, got 1)', 'type': 'value_error'}]"
    )

    # 3D bounding box is allowed
    # body = {"query": {"bbox": [100.0, 0.0, 0.0, 105.0, 1.0, 1.0]}}
    # resp = app_client.post("/search", json=body)
    # assert resp.status_code == 400

    params = {"bbox": "100.0,0.0,0.0,105.0"}
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "1 validation error for SearchPostRequest\nbbox\n  Maximum longitude must be greater than minimum longitude (type=value_error)"
    )


def test_filter_crs_in_epsg25832_should_not_affect_bbox_in_epsg4326(
    app_client, load_test_data
):
    """Test that bbox is unaffected of filter-crs params if filter-params is specified and result is in http://www.opengis.net/def/crs/OGC/1.3/CRS84 (crsExtension)"""
    test_item = load_test_data("test_item.json")
    params = {
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
        "filter-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "ids": test_item["id"],
        "collections": test_item["collection"],
    }
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200

    resp_json = resp.json()
    assert resp_json["features"][0]["id"] == test_item["id"]
    assert (
        resp_json["features"][0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def test_crs_epsg25832(app_client):
    """Test response geometry in crs 25832"""
    params = {"crs": "http://www.opengis.net/def/crs/EPSG/0/25832"}
    resp = app_client.get("/search", params=params)
    resp_json = resp.json()
    assert (
        resp_json["features"][0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/EPSG/0/25832"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/EPSG/0/25832"

    body = {"crs": "http://www.opengis.net/def/crs/EPSG/0/25832"}
    resp = app_client.post("/search", json=body)
    resp_json = resp.json()
    assert (
        resp_json["features"][0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/EPSG/0/25832"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/EPSG/0/25832"


def test_crs_epsg4326(app_client):
    """Test response geometry in crs 4326"""
    params = {"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}
    resp = app_client.get(f"/search", params=params)
    resp_json = resp.json()
    assert (
        resp_json["features"][0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"

    body = {"crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"}
    resp = app_client.post("/search", json=body)
    resp_json = resp.json()
    assert (
        resp_json["features"][0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def test_filter_crs_epsg4326(app_client, load_test_data):
    """Test filter with default filter geometry, result in supported crs (crsExtension)"""
    test_item = load_test_data("test_item.json")
    body = {
        "collections": [test_item["collection"]],
        "filter": {"intersects": [{"property": "geometry"}, test_item["geometry"]]},
        "filter-crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "limit": 200,
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 200

    resp_json = resp.json()
    matching_feat = [x for x in resp_json["features"] if x["id"] == test_item["id"]]
    assert len(matching_feat) == 1
    # Is the geometry "almost" the same. (Which is good enough for this assesment)
    assert shape(matching_feat[0]["geometry"]).equals_exact(
        shape(test_item["geometry"]), 1e-6
    )
    assert (
        resp_json["features"][0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def test_filter_crs_wrong_filter_crs_epsg25832(app_client, load_test_data):
    """Test filter with default filter geometry, result should return zero items (crsExtension)"""
    test_item = load_test_data("test_item.json")
    body = {
        "collections": [test_item["collection"]],
        "filter": {"intersects": [{"property": "geometry"}, test_item["geometry"]]},
        "filter-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
        "limit": 200,
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 200

    assert resp.json()["context"]["returned"] == 0
    assert resp.json()["context"]["matched"] == 0
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def test_filter_crs_epsg25832(app_client, load_test_data):
    """Test filter with filter geometry in epsg 25832, result in supported crs (crsExtension)"""
    test_item = load_test_data("test_item.json")

    body = {
        "collections": [test_item["collection"]],
        "filter": {
            "intersects": [
                {"property": "geometry"},
                {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [491947.05803559424, 6187696.446854165],
                            [494005.4428012192, 6187704.692947916],
                            [494008.1595980942, 6186303.810135417],
                            [491957.0150668442, 6186293.536697917],
                            [491947.05803559424, 6187696.446854165],
                        ]
                    ],
                },
            ]
        },
        "filter-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "limit": 10,
    }
    resp = app_client.post("/search", json=body)
    assert resp.status_code == 200

    resp_json = resp.json()
    matching_feat = [x for x in resp_json["features"] if x["id"] == test_item["id"]]
    assert len(matching_feat) == 1
    assert (
        resp_json["features"][0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/EPSG/0/25832"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/EPSG/0/25832"


def test_filter_get_crs_epsg25832(app_client, load_test_data):
    """Test filter with filter geometry in epsg 25832, result in supported crs (crsExtension)"""
    test_item = load_test_data("test_item.json")

    params = {
        "collections": [test_item["collection"]],
        "filter": json.dumps(
            {
                "intersects": [
                    {"property": "geometry"},
                    {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [491947.05803559424, 6187696.446854165],
                                [494005.4428012192, 6187704.692947916],
                                [494008.1595980942, 6186303.810135417],
                                [491957.0150668442, 6186293.536697917],
                                [491947.05803559424, 6187696.446854165],
                            ]
                        ],
                    },
                ]
            }
        ),
        "filter-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "limit": 10,
    }
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200

    resp_json = resp.json()
    matching_feat = [x for x in resp_json["features"] if x["id"] == test_item["id"]]
    assert len(matching_feat) == 1
    assert (
        resp_json["features"][0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/EPSG/0/25832"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/EPSG/0/25832"


def test_single_item_get_bbox_with_bbox_crs(app_client, load_test_data):
    """Test get single item with bbox in supported crs result in default crs (crsExtension)"""
    test_item = load_test_data("test_item.json")
    params = {
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
        "crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
    }
    resp = app_client.get(
        f'/collections/{test_item["collection"]}/items/{test_item["id"]}', params=params
    )
    assert resp.status_code == 200

    resp_json = resp.json()
    # TODO rewrite assertion. It could check if response json bbox actually is changed to the correct converted bbox
    assert resp_json["bbox"] != test_item["bbox"]
    assert resp_json["bbox"] == [
        491947.05803559424,
        6186293.536697917,
        494008.1595980942,
        6187704.692947916,
    ]
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/EPSG/0/25832"


def test_collection_item_get_bbox_with_bbox_crs(app_client, load_test_data):
    """Test get single item with bbox in supported crs result in default crs (crsExtension)"""
    test_item = load_test_data("test_item.json")
    params = {
        "bbox": ",".join([str(coord) for coord in test_item["bbox"]]),
        "crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "limit": 200,
    }
    resp = app_client.get(f'/collections/{test_item["collection"]}/items', params=params)
    assert resp.status_code == 200

    resp_json = resp.json()

    matching_feat = [x for x in resp_json["features"] if x["id"] == test_item["id"]]
    assert len(matching_feat) == 1
    assert matching_feat[0]["bbox"] != test_item["bbox"]
    assert (
        matching_feat[0]["properties"]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/EPSG/0/25832"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/EPSG/0/25832"


def test_single_item_get_bbox_crs_with_crs(app_client, load_test_data):
    """Test get with bbox in supported crs with result in supported crs (crsExtension)"""

    test_item = load_test_data("test_item.json")
    bbox = [492283, 6195600, 492283, 6195605]
    params = {
        "bbox": ",".join([str(coord) for coord in bbox]),
        "bbox-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
    }
    resp = app_client.get(f'/collections/{test_item["collection"]}/items', params=params)
    assert resp.status_code == 200

    resp_json = resp.json()
    assert resp_json["context"]["matched"] == 50
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def test_item_search_bbox_crs_with_crs(app_client, load_test_data):
    """Test get with default bbox, result in supported crs(crsExtension)"""
    test_item = load_test_data("test_item.json")
    bbox = [491947.05803559424, 6186293.536697917, 494008.1595980942, 6187704.692947916]
    params = {
        "bbox": ",".join([str(coord) for coord in bbox]),
        "bbox-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "limit": 10,
    }
    resp = app_client.get(f'/collections/{test_item["collection"]}/items', params=params)
    assert resp.status_code == 200

    resp_json = resp.json()

    matching_feat = [x for x in resp_json["features"] if x["id"] == test_item["id"]]
    assert len(matching_feat) == 1
    assert matching_feat[0]["bbox"] != test_item["bbox"]
    assert (
        matching_feat[0]["properties"]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/EPSG/0/25832"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/EPSG/0/25832"


def test_item_post_bbox_with_bbox_crs(app_client, load_test_data):
    """Test post with bbox in supported crs result in default crs (crsExtension)"""
    test_item = load_test_data("test_item.json")
    bbox = [492283, 6195600, 492283, 6195605]
    params = {
        "bbox": bbox,
        "ids": [test_item["id"]],
        "collections": [test_item["collection"]],
        "bbox-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "limit": 1,
    }
    resp = app_client.post(f"/search", json=params)
    assert resp.status_code == 200

    resp_json = resp.json()

    matching_feat = [x for x in resp_json["features"] if x["id"] == test_item["id"]]
    assert len(matching_feat) == 1
    assert matching_feat[0]["bbox"] == pytest.approx(test_item["bbox"])
    assert (
        matching_feat[0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


def test_item_post_bbox_with_crs(app_client, load_test_data):
    """Test post with default bbox, result in supported crs(crsExtension)"""
    test_item = load_test_data("test_item.json")
    bbox = [492283, 6195600, 492283, 6195605]
    params = {
        "bbox": bbox,
        "ids": [test_item["id"]],
        "collections": [test_item["collection"]],
        "bbox-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "limit": 1,
    }
    resp = app_client.post(f"/search", json=params)
    assert resp.status_code == 200

    resp_json = resp.json()

    matching_feat = [x for x in resp_json["features"] if x["id"] == test_item["id"]]
    assert len(matching_feat) == 1
    assert matching_feat[0]["bbox"] != test_item["bbox"]
    assert (
        matching_feat[0]["crs"]["properties"]["name"]
        == "http://www.opengis.net/def/crs/EPSG/0/25832"
    )
    assert resp.headers["content-crs"] == "http://www.opengis.net/def/crs/EPSG/0/25832"


def test_item_wrong_crs(app_client, load_test_data):
    """Test post with default bbox, response should be an error defining what supported that is crs(crsExtension)"""
    test_item = load_test_data("test_item.json")
    bbox = [492283, 6195600, 492283, 6195605]
    params = {
        "bbox": bbox,
        "ids": [test_item["id"]],
        "collections": [test_item["collection"]],
        "bbox-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "crs": "wrong-crs",
        "limit": 1,
    }
    resp = app_client.post(f"/search", json=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "[{'loc': ('body', 'crs'), 'msg': \"unexpected value; permitted: 'http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832'\", 'type': 'value_error.const', 'ctx': {'given': 'wrong-crs', 'permitted': ('http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832')}}]"
    )

    """Test get with default bbox, response should be an error defining what supported that is crs(crsExtension)"""
    params = {
        "bbox": ",".join([str(coord) for coord in bbox]),
        "bbox-crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "crs": "wrong-crs",
        "limit": 1,
    }

    resp = app_client.get(f"/search", params=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "1 validation error for SearchPostRequest\ncrs\n  unexpected value; permitted: 'http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832' (type=value_error.const; given=wrong-crs; permitted=('http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832'))"
    )

    resp = app_client.get(f'/collections/{test_item["collection"]}/items', params=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "CRS provided for argument crs is invalid, valid options are: http://www.opengis.net/def/crs/OGC/1.3/CRS84, http://www.opengis.net/def/crs/EPSG/0/25832"
    )

    resp = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}", params=params
    )
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "CRS provided for argument crs is invalid, valid options are: http://www.opengis.net/def/crs/OGC/1.3/CRS84, http://www.opengis.net/def/crs/EPSG/0/25832"
    )


def test_item_wrong_bbox_crs(app_client, load_test_data):
    """Test post with default bbox, response should be an error defining what supported that is crs(crsExtension)"""
    test_item = load_test_data("test_item.json")
    bbox = [492283, 6195600, 492283, 6195605]
    params = {
        "bbox": bbox,
        "ids": [test_item["id"]],
        "collections": [test_item["collection"]],
        "bbox-crs": "wrong-bbox-crs",
        "crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "limit": 1,
    }
    resp = app_client.post(f"/search", json=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "[{'loc': ('body', 'bbox-crs'), 'msg': \"unexpected value; permitted: 'http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832'\", 'type': 'value_error.const', 'ctx': {'given': 'wrong-bbox-crs', 'permitted': ('http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832')}}]"
    )

    """Test get with default bbox, response should be an error defining what supported that is crs(crsExtension)"""
    params = {
        "bbox": ",".join([str(coord) for coord in bbox]),
        "bbox-crs": "wrong-bbox-crs",
        "crs": "http://www.opengis.net/def/crs/EPSG/0/25832",
        "limit": 1,
    }

    resp = app_client.get(f"/search", params=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "1 validation error for SearchPostRequest\nbbox-crs\n  unexpected value; permitted: 'http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832' (type=value_error.const; given=wrong-bbox-crs; permitted=('http://www.opengis.net/def/crs/OGC/1.3/CRS84', 'http://www.opengis.net/def/crs/EPSG/0/25832'))"
    )

    resp = app_client.get(f'/collections/{test_item["collection"]}/items', params=params)
    assert resp.status_code == 400
    resp_json = resp.json()
    assert resp_json["code"] == "RequestValidationError"
    assert (
        resp_json["description"]
        == "CRS provided for argument bbox_crs is invalid, valid options are: http://www.opengis.net/def/crs/OGC/1.3/CRS84, http://www.opengis.net/def/crs/EPSG/0/25832"
    )


@pytest.mark.skip(reason="href_builder can not be initi")
def test_conformance_classes_configurable():
    """Test conformance class configurability"""
    landing = LandingPageMixin()
    landing_page = landing._landing_page(
        base_url="http://test/test",
        conformance_classes=["this is a test"],
        extension_schemas=[],
    )
    assert landing_page["conformsTo"][0] == "this is a test"

    # Update environment to avoid key error on client instantiation
    os.environ["READER_CONN_STRING"] = "testing"
    os.environ["WRITER_CONN_STRING"] = "testing"
    client = CoreCrudClient(base_conformance_classes=["this is a test"])
    assert client.conformance_classes()[0] == "this is a test"


def test_search_datetime_validation_errors(app_client):
    bad_datetimes = [
        "37-01-01T12:00:27.87Z",
        "1985-13-12T23:20:50.52Z",
        "1985-12-32T23:20:50.52Z",
        "1985-12-01T25:20:50.52Z",
        "1985-12-01T00:60:50.52Z",
        "1985-12-01T00:06:61.52Z",
        "1990-12-31T23:59:61Z",
        "1986-04-12T23:20:50.52Z/1985-04-12T23:20:50.52Z",
    ]
    for dt in bad_datetimes:
        # body = {"query": {"datetime": dt}}
        body = {"datetime": dt}
        resp = app_client.post("/search", json=body)
        assert resp.status_code == 400

        resp = app_client.get("/search?datetime={}".format(dt))
        assert resp.status_code == 400


def test_get_item_forwarded_header(app_client, load_test_data):
    test_item = load_test_data("test_item.json")
    # app_client.post(f"/collections/{test_item['collection']}/items", json=test_item)
    get_item = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        # headers={"Forwarded": "proto=https;host=testserver:1234"},
        headers={"Forwarded": "proto=https;host=api.dataforsyningen.dk"},
    )
    print(get_item.json()["links"])
    for link in get_item.json()["links"]:
        # assert link["href"].startswith("https://testserver:1234/")
        if link["href"].startswith("https://api.dataforsyningen.dk/"):
            assert link["href"].startswith("https://api.dataforsyningen.dk/")
        else:
            # We have a license URL that does not start with the same host as the rest of the URL's
            assert link["href"].startswith("https://kds.dk")


def test_get_item_x_forwarded_headers(app_client, load_test_data):
    test_item = load_test_data("test_item.json")
    # app_client.post(f"/collections/{test_item['collection']}/items", json=test_item)
    get_item = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        # headers={
        #     "X-Forwarded-Port": "1234",
        #     "X-Forwarded-Proto": "https",
        # },
        headers={
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "api.dataforsyningen.dk",
            "X-Forwarded-Prefix": "/rest/skraafoto_api/v2/",
        },
    )
    for link in get_item.json()["links"]:
        # assert link["href"].startswith("https://testserver:1234/")
        if link["href"].startswith(
            "https://api.dataforsyningen.dk/rest/skraafoto_api/v2/"
        ):
            assert link["href"].startswith(
                "https://api.dataforsyningen.dk/rest/skraafoto_api/v2/"
            )
        elif link["href"].startswith("https://cdn.dataforsyningen.dk/skraafoto_server/"):
            assert link["href"].startswith(
                "https://cdn.dataforsyningen.dk/skraafoto_server/"
            )
        else:
            # We have a license URL that does not start with the same host as the rest of the URL's
            assert link["href"].startswith("https://kds.dk")


def test_get_item_duplicate_forwarded_headers(app_client, load_test_data):
    test_item = load_test_data("test_item.json")
    app_client.post(f"/collections/{test_item['collection']}/items", json=test_item)
    get_item = app_client.get(
        f"/collections/{test_item['collection']}/items/{test_item['id']}",
        headers={
            "Forwarded": "proto=https;host=api.dataforsyningen.dk",
            "X-Forwarded-Proto": "http",
        },
    )
    for link in get_item.json()["links"]:
        # assert link["href"].startswith("https://testserver:1234/")
        if link["href"].startswith("https://api.dataforsyningen.dk/"):
            assert link["href"].startswith("https://api.dataforsyningen.dk/")
        else:
            # We have a license URL that does not start with the same host as the rest of the URL's
            assert link["href"].startswith("https://kds.dk")
