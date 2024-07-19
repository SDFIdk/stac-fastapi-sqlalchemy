from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pytest
import orjson

from ..conftest import MockStarletteRequest

STAC_CORE_ROUTES = [
    "GET /",
    "GET /collections",
    "GET /collections/{collection_id}",
    "GET /collections/{collection_id}/items",
    "GET /collections/{collection_id}/items/{item_id}",
    "GET /conformance",
    "GET /search",
    "POST /search",
]

# Database is readonly
# STAC_TRANSACTION_ROUTES = [
#     "DELETE /collections/{collection_id}",
#     "DELETE /collections/{collection_id}/items/{item_id}",
#     "POST /collections",
#     "POST /collections/{collection_id}/items",
#     "PUT /collections",
#     "PUT /collections/{collection_id}/items/{item_id}",
# ]


def test_post_search_content_type(app_client):
    params = {"limit": 1}
    resp = app_client.post("search", json=params)
    assert resp.headers["content-type"] == "application/geo+json"


def test_get_search_content_type(app_client):
    resp = app_client.get("search")
    assert resp.headers["content-type"] == "application/geo+json"


def test_api_headers(app_client):
    resp = app_client.get("/api")
    assert (
        resp.headers["content-type"] == "application/vnd.oai.openapi+json;version=3.0"
    )
    assert resp.status_code == 200


def test_core_router(api_client):
    core_routes = set(STAC_CORE_ROUTES)
    api_routes = set(
        [f"{list(route.methods)[0]} {route.path}" for route in api_client.app.routes]
    )
    assert not core_routes - api_routes


def test_landing_page_stac_extensions(app_client):
    resp = app_client.get("/")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert not resp_json["stac_extensions"]


@pytest.mark.skip(reason="Database is readonly")
def test_transactions_router(api_client):
    transaction_routes = set(STAC_TRANSACTION_ROUTES)
    api_routes = set(
        [f"{list(route.methods)[0]} {route.path}" for route in api_client.app.routes]
    )
    assert not transaction_routes - api_routes


@pytest.mark.skip(reason="Database is readonly")
def test_app_transaction_extension(app_client, load_test_data):
    item = load_test_data("test_item.json")
    resp = app_client.post(f"/collections/{item['collection']}/items", json=item)
    assert resp.status_code == 200


# def test_app_search_response(load_test_data, app_client, postgres_transactions):
def test_app_search_response(load_test_data, app_client):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #    item["collection"], item, request=MockStarletteRequest
    # )

    # resp = app_client.get("/search", params={"collections": ["test-collection"]})
    resp = app_client.get("/search", params={"collections": [item["collection"]]})
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/geo+json"
    resp_json = resp.json()

    assert resp_json.get("type") == "FeatureCollection"
    # stac_version and stac_extensions were removed in v1.0.0-beta.3
    assert resp_json.get("stac_version") is None
    assert resp_json.get("stac_extensions") is None


def test_app_post_search_response(load_test_data,app_client):
    item = load_test_data("test_item.json")
    resp = app_client.post("/search", json={"collections": [item["collection"]]})
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/geo+json"
    resp_json = resp.json()

    assert resp_json.get("type") == "FeatureCollection"
    # stac_version and stac_extensions were removed in v1.0.0-beta.3
    assert resp_json.get("stac_version") is None
    assert resp_json.get("stac_extensions") is None


@pytest.mark.skip(reason="Database is readonly")
def test_app_search_response_multipolygon(
    load_test_data, app_client, postgres_transactions
):
    item = load_test_data("test_item_multipolygon.json")
    postgres_transactions.create_item(
        item["collection"], item, request=MockStarletteRequest
    )

    resp = app_client.get("/search", params={"collections": ["test-collection"]})
    assert resp.status_code == 200
    resp_json = resp.json()

    assert resp_json.get("type") == "FeatureCollection"
    assert resp_json.get("features")[0]["geometry"]["type"] == "MultiPolygon"


@pytest.mark.skip(reason="Database is readonly")
def test_app_search_response_geometry_null(
    load_test_data, app_client, postgres_transactions
):
    item = load_test_data("test_item_geometry_null.json")
    postgres_transactions.create_item(
        item["collection"], item, request=MockStarletteRequest
    )

    resp = app_client.get("/search", params={"collections": ["test-collection"]})
    assert resp.status_code == 200
    resp_json = resp.json()

    assert resp_json.get("type") == "FeatureCollection"
    assert resp_json.get("features")[0]["geometry"] is None
    assert resp_json.get("features")[0]["bbox"] is None


#def test_app_context_extension(load_test_data, app_client, postgres_transactions):
def test_app_context_extension(load_test_data, app_client):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    # resp = app_client.get("/search", params={"collections": ["test-collection"]})
    resp = app_client.get("/search", params={"collections": [item["collection"]]})
    assert resp.status_code == 200
    resp_json = resp.json()
    assert "context" in resp_json
    #assert resp_json["context"]["returned"] == resp_json["context"]["matched"] == 1
    if resp_json["context"]["returned"] == resp_json["context"]["limit"]:
        assert resp_json["context"]["limit"] < resp_json["context"]["matched"]
    else:
        assert resp_json["context"]["limit"] > resp_json["context"]["matched"]


@pytest.mark.skip(reason="Field Extension switched off")
def test_app_fields_extension(load_test_data, app_client, postgres_transactions):
    item = load_test_data("test_item.json")
    postgres_transactions.create_item(
        item["collection"], item, request=MockStarletteRequest
    )

    resp = app_client.get("/search", params={"collections": ["test-collection"]})
    assert resp.status_code == 200
    resp_json = resp.json()
    assert list(resp_json["features"][0]["properties"]) == ["datetime"]


@pytest.mark.skip(reason="Query Extension switched off")
def test_app_query_extension_gt(load_test_data, app_client, postgres_transactions):
    test_item = load_test_data("test_item.json")
    postgres_transactions.create_item(
        test_item["collection"], test_item, request=MockStarletteRequest
    )

    params = {"query": {"proj:epsg": {"gt": test_item["properties"]["proj:epsg"]}}}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0

    params["query"] = quote_plus(orjson.dumps(params["query"]))
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


@pytest.mark.skip(reason="Query Extension switched off")
def test_app_query_extension_gte(load_test_data, app_client, postgres_transactions):
    test_item = load_test_data("test_item.json")
    postgres_transactions.create_item(
        test_item["collection"], test_item, request=MockStarletteRequest
    )

    params = {"query": {"proj:epsg": {"gte": test_item["properties"]["proj:epsg"]}}}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


@pytest.mark.skip(reason="Query Extension switched off")
def test_app_query_extension_limit_eq0(app_client):
    params = {"limit": 0}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 400


def test_app_filter_extension_limit_eq0(app_client):
    params = {"limit": 0}
    
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 400

    resp = app_client.post("/search", json=params)
    assert resp.status_code == 400


@pytest.mark.skip(reason="Query Extension switched off")
def test_app_query_extension_limit_lt0(
    load_test_data, app_client, postgres_transactions
):
    item = load_test_data("test_item.json")
    postgres_transactions.create_item(
        item["collection"], item, request=MockStarletteRequest
    )

    params = {"limit": -1}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 400


def test_app_filter_extension_limit_lt0(app_client):
    params = {"limit": -1}

    resp = app_client.get("/search", params=params)
    assert resp.status_code == 400

    resp = app_client.post("/search", json=params)
    assert resp.status_code == 400


@pytest.mark.skip(reason="Query Extension switched off")
def test_app_query_extension_limit_gt10000(
    load_test_data, app_client, postgres_transactions
):
    item = load_test_data("test_item.json")
    postgres_transactions.create_item(
        item["collection"], item, request=MockStarletteRequest
    )

    params = {"limit": 10001}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200


def test_app_filter_extension_limit_gt10000(app_client):
    params = {"limit": 10001}

    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200

    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200


@pytest.mark.skip(reason="Query Extension switched off")
def test_app_query_extension_limit_10000(
    load_test_data, app_client, postgres_transactions
):
    item = load_test_data("test_item.json")
    postgres_transactions.create_item(
        item["collection"], item, request=MockStarletteRequest
    )

    params = {"limit": 10000}
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200


def test_app_filter_extension_limit_gt10000(app_client):
    params = {"limit": 10000}

    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200

    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200


def test_app_sort_extension(load_test_data, app_client, postgres_transactions):
    # first_item = load_test_data("test_item.json")
    # item_date = datetime.strptime(
    #     first_item["properties"]["datetime"], "%Y-%m-%dT%H:%M:%SZ"
    # )
    # postgres_transactions.create_item(
    #     first_item["collection"], first_item, request=MockStarletteRequest
    # )

    # second_item = load_test_data("test_item.json")
    # second_item["id"] = "another-item"
    # another_item_date = item_date - timedelta(days=1)
    # second_item["properties"]["datetime"] = another_item_date.strftime(
    #     "%Y-%m-%dT%H:%M:%SZ"
    # )
    # postgres_transactions.create_item(
    #     second_item["collection"], second_item, request=MockStarletteRequest
    # )

    # params = {
    #     "collections": [first_item["collection"]],
    #     "sortby": [{"field": "datetime", "direction": "desc"}],
    # }
    
    get_params = {
        "collections": "skraafotos2021",
        "sortby": "+datetime",
    }

    post_params = {
        "collections": ["skraafotos2021"],
        "sortby": [{"field": "datetime", "direction": "asc"}],
    }

    resp = app_client.get("/search", params=get_params)
    assert resp.status_code == 200
    resp_json = resp.json()
    # assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][0]["id"] == "2021_85_45_1_0045_00000003"
    # assert resp_json["features"][1]["id"] == second_item["id"]
    assert resp_json["features"][1]["id"] == "2021_85_45_2_0045_00000003"

    # resp = app_client.post("/search", json=params)
    resp = app_client.post("/search", json=post_params)
    assert resp.status_code == 200
    resp_json = resp.json()
    # assert resp_json["features"][0]["id"] == first_item["id"]
    assert resp_json["features"][0]["id"] == "2021_85_45_1_0045_00000003"
    # assert resp_json["features"][1]["id"] == second_item["id"]
    assert resp_json["features"][1]["id"] == "2021_85_45_2_0045_00000003"


#def test_search_invalid_date(load_test_data, app_client, postgres_transactions):
def test_search_invalid_date(load_test_data, app_client):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    # params = {
    #     "datetime": "2020-XX-01/2020-10-30",
    #     "collections": [item["collection"]],
    # }

    params = {
        "datetime": "2021-XX-01/2021-10-30",
        "collections": [item["collection"]],
    }

    resp = app_client.get("/search", params=params)
    assert resp.status_code == 400

    resp = app_client.post("/search", json=params)
    assert resp.status_code == 400


#def test_search_point_intersects(load_test_data, app_client, postgres_transactions):
def test_search_point_intersects(load_test_data, app_client):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    # new_coordinates = list()
    # for coordinate in item["geometry"]["coordinates"][0]:
    #     new_coordinates.append([coordinate[0] * -1, coordinate[1] * -1])
    # item["id"] = "test-item-other-hemispheres"
    # item["geometry"]["coordinates"] = [new_coordinates]
    # item["bbox"] = list(value * -1 for value in item["bbox"])
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    # point = [150.04, -33.14]
    point = [8.4570, 56.24298]
    intersects = {"type": "Point", "coordinates": point}

    params = {
        "intersects": intersects,
        "collections": [item["collection"]],
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    # assert len(resp_json["features"]) == 1
    assert len(resp_json["features"]) == 10

    params["intersects"] = orjson.dumps(params["intersects"]).decode("utf-8")
    resp = app_client.get("/search", params=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    # assert len(resp_json["features"]) == 1
    assert len(resp_json["features"]) == 10


#def test_datetime_non_interval(load_test_data, app_client, postgres_transactions):
def test_datetime_non_interval(load_test_data, app_client):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )
    alternate_formats = [
        "2021-11-22T10:03:41+00:00",
        "2021-11-22T10:03:41.00Z",
        "2021-11-22T10:03:41Z",
        "2021-11-22T10:03:41.00+00:00",
    ]
    for date in alternate_formats:
        params = {
            "datetime": date,
            "collections": [item["collection"]],
        }

        resp = app_client.post("/search", json=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        # datetime is returned in this format "2021-11-22T10:03:41Z"
        assert resp_json["features"][0]["properties"]["datetime"][0:19] == date[0:19]

        resp = app_client.get("/search", params=params)
        assert resp.status_code == 200
        resp_json = resp.json()
        # datetime is returned in this format "2021-11-22T10:03:41Z"
        assert resp_json["features"][0]["properties"]["datetime"][0:19] == date[0:19]


# def test_bbox_3d(load_test_data, app_client, postgres_transactions):
def test_bbox_3d(load_test_data, app_client):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    # australia_bbox = [106.343365, -47.199523, 0.1, 168.218365, -19.437288, 0.1]
    danish_bbox = [9.65, 55.21, 9.67, 55.22]
    params = {
        # "bbox": australia_bbox,
        "bbox": danish_bbox,
        "collections": [item["collection"]],
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10

    get_params = {
        # "bbox": australia_bbox,
        "bbox": "9.65, 55.21, 9.67, 55.22",
        "collections": [item["collection"]],
    }

    resp = app_client.get("/search", params=get_params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10


# def test_search_line_string_intersects(
#     load_test_data, app_client, postgres_transactions
# ):
def test_search_line_string_intersects(
    load_test_data, app_client
):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    #line = [[150.04, -33.14], [150.22, -33.89]]
    line = [[10.91, 55.91],[12.09, 56.26]]
    intersects = {"type": "LineString", "coordinates": line}

    params = {
        "intersects": intersects,
        "collections": [item["collection"]],
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10

    get_params = {
        "intersects": orjson.dumps(params["intersects"]).decode("utf-8"),
        "collections": [item["collection"]],
    }

    resp = app_client.get("/search", params=get_params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 10


@pytest.mark.skip(reason="Field Extension switched off")
def test_app_fields_extension_return_all_properties(
    load_test_data, app_client, postgres_transactions
):
    item = load_test_data("test_item.json")
    postgres_transactions.create_item(
        item["collection"], item, request=MockStarletteRequest
    )

    resp = app_client.get(
        "/search", params={"collections": ["test-collection"], "fields": "properties"}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    feature = resp_json["features"][0]
    assert len(feature["properties"]) >= len(item["properties"])
    for expected_prop, expected_value in item["properties"].items():
        if expected_prop in ("datetime", "created", "updated"):
            assert feature["properties"][expected_prop][0:19] == expected_value[0:19]
        else:
            assert feature["properties"][expected_prop] == expected_value


# def test_landing_forwarded_header(load_test_data, app_client, postgres_transactions):
def test_landing_forwarded_header(app_client):
    # item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    response = app_client.get(
        "/",
        headers={
            "Forwarded": "proto=https;host=test:1234",
            "X-Forwarded-Proto": "http",
            "X-Forwarded-Port": "4321",
            "X-Forwarded-Host": "test"
        },
    ).json()
    for link in response["links"]:
        assert link["href"].startswith("https://test:1234/")


# def test_app_search_response_forwarded_header(
#     load_test_data, app_client, postgres_transactions
# ):
def test_app_search_response_forwarded_header(
    load_test_data, app_client
):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    resp = app_client.get(
        "/search",
        # params={"collections": ["test-collection"]},
        params={"collections": [item["collection"]]},
        # headers={"Forwarded": "proto=https;host=testserver:1234"},
        headers={"Forwarded": "proto=https;host=api.dataforsyningen.dk"},
    )
    for feature in resp.json()["features"]:
        for link in feature["links"]:
            # assert link["href"].startswith("https://testserver:1234/")
            if link["href"].startswith("https://api.dataforsyningen.dk/"):
                assert link["href"].startswith("https://api.dataforsyningen.dk/")
            else:
                # We have a license URL that does not start with the same host as the rest of the URL's
                assert link["href"].startswith("https://sdfi.dk/")


# def test_app_search_response_x_forwarded_headers(
#     load_test_data, app_client, postgres_transactions
# ):
def test_app_search_response_x_forwarded_headers(
    load_test_data, app_client
):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    resp = app_client.get(
        "/search",
        # params={"collections": ["test-collection"]},
        params={"collections": [item["collection"]]},
        headers={
            #"X-Forwarded-Port": "1234",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "api.dataforsyningen.dk"
        },
    )
    for feature in resp.json()["features"]:
        for link in feature["links"]:
            # assert link["href"].startswith("https://testserver:1234/")
            if link["href"].startswith("https://api.dataforsyningen.dk/"):
                assert link["href"].startswith("https://api.dataforsyningen.dk/")
            else:
                # We have a license URL that does not start with the same host as the rest of the URL's
                assert link["href"].startswith("https://sdfi.dk/")


# def test_app_search_response_duplicate_forwarded_headers(
#     load_test_data, app_client, postgres_transactions
# ):
def test_app_search_response_duplicate_forwarded_headers(
    load_test_data, app_client
):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    resp = app_client.get(
        "/search",
        # params={"collections": ["test-collection"]},
        params={"collections": [item["collection"]]},
        headers={
            #"Forwarded": "proto=https;host=testserver:1234"
            "Forwarded": "proto=https;host=api.dataforsyningen.dk",
            #"X-Forwarded-Port": "4321",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "api.dataforsyningen.dk"
        },
    )
    for feature in resp.json()["features"]:
        for link in feature["links"]:
            # assert link["href"].startswith("https://testserver:1234/")
            if link["href"].startswith("https://api.dataforsyningen.dk/"):
                assert link["href"].startswith("https://api.dataforsyningen.dk/")
            else:
                # We have a license URL that does not start with the same host as the rest of the URL's
                assert link["href"].startswith("https://sdfi.dk/")


def test_get_features_content_type(app_client, load_test_data):
    item = load_test_data("test_item.json")
    resp = app_client.get(f"collections/{item['collection']}/items")
    assert resp.headers["content-type"] == "application/geo+json"


#def test_get_feature_content_type(app_client, load_test_data, postgres_transactions):
def test_get_feature_content_type(app_client, load_test_data):
    item = load_test_data("test_item.json")
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )
    resp = app_client.get(f"collections/{item['collection']}/items/{item['id']}")
    assert resp.headers["content-type"] == "application/geo+json"


# def test_item_collection_filter_bbox(load_test_data, app_client, postgres_transactions):
def test_item_collection_filter_bbox(load_test_data, app_client):
    item = load_test_data("test_item.json")
    collection = item["collection"]
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    #bbox = "100,-50,170,-20"
    bbox = "9.65, 55.21, 9.67, 55.22"
    resp = app_client.get(f"/collections/{collection}/items", params={"bbox": bbox})
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1

    bbox = "1,2,3,4"
    resp = app_client.get(f"/collections/{collection}/items", params={"bbox": bbox})
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0


# def test_item_collection_filter_datetime(
#     load_test_data, app_client, postgres_transactions
# ):
def test_item_collection_filter_datetime(
    load_test_data, app_client
):
    item = load_test_data("test_item.json")
    collection = item["collection"]
    # postgres_transactions.create_item(
    #     item["collection"], item, request=MockStarletteRequest
    # )

    datetime_range = "2020-01-01T00:00:00.00Z/.."
    resp = app_client.get(
        f"/collections/{collection}/items", params={"datetime": datetime_range}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    # assert len(resp_json["features"]) == 1
    assert len(resp_json["features"]) == 10

    datetime_range = "2018-01-01T00:00:00.00Z/2019-01-01T00:00:00.00Z"
    resp = app_client.get(
        f"/collections/{collection}/items", params={"datetime": datetime_range}
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 0
