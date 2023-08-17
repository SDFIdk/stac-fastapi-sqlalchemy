"""Serializers."""
import abc
import json
from datetime import timedelta, timezone
from typing import Any, Dict, TypedDict
import urllib.parse

import attr
import geoalchemy2 as ga

from pystac.utils import datetime_to_str

from stac_fastapi.sqlalchemy.config import SqlalchemySettings

from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.config import Settings
from stac_fastapi.types.links import BaseHrefBuilder, CollectionLinks, ItemLinks, resolve_links
from stac_fastapi.types.rfc3339 import now_to_rfc3339_str, rfc3339_str_to_datetime

from stac_fastapi.sqlalchemy.models import database

from stac_pydantic.shared import DATETIME_RFC339

DATE_RFC339 = "%Y-%m-%d"
UTC_TIMEZONE = timezone(timedelta(0))
settings = SqlalchemySettings()


def _add_query_params(url, params):
    """Combines URL with params"""
    if not params:
        return url
    url_parts = list(urllib.parse.urlparse(url))
    query = dict(urllib.parse.parse_qsl(url_parts[4]))
    query.update(params)
    url_parts[4] = urllib.parse.urlencode(query)
    return urllib.parse.urlunparse(url_parts)


@attr.s  # type:ignore
class Serializer(abc.ABC):
    """Defines serialization methods between the API and the data model."""

    @classmethod
    @abc.abstractmethod
    #def db_to_stac(cls, db_model: database.BaseModel, base_url: str) -> TypedDict:
    def db_to_stac(cls, db_model: database.BaseModel, base_url: BaseHrefBuilder) -> TypedDict:
        """Transform database model to stac."""
        ...

    @classmethod
    @abc.abstractmethod
    def stac_to_db(
        cls, stac_data: TypedDict, exclude_geometry: bool = False
    ) -> database.BaseModel:
        """Transform stac to database model."""
        ...

    @classmethod
    def row_to_dict(cls, db_model: database.BaseModel):
        """Transform a database model to it's dictionary representation."""
        d = {}
        for column in db_model.__table__.columns:
            value = getattr(db_model, column.name)
            if value:
                d[column.name] = value
        return d


class ItemSerializer(Serializer):
    """Serialization methods for STAC items."""

    @classmethod
    def _add_if_not_none(cls, dict: Dict, key: str, val: Any):
        """Adds value to dictionary with specified key if value is not None"""
        if val is not None:
            dict[key] = val

    @classmethod
    #def db_to_stac(cls, db_model: database.Item, base_url: str) -> stac_types.Item:
    def db_to_stac(cls, db_model: database.Item, hrefbuilder: BaseHrefBuilder) -> stac_types.Item:
        """Transform database model to stac item."""
        properties = db_model.properties.copy()
        indexed_fields = Settings.get().indexed_fields
        for field in indexed_fields:
            # Use getattr to accommodate extension namespaces
            field_value = getattr(db_model, field.split(":")[-1])
            if field == "datetime":
                # field_value = datetime_to_str(field_value)
                field_value = field_value.astimezone(timezone(timedelta(0))).strftime(
                    DATETIME_RFC339
                )
            properties[field] = field_value
        item_id = db_model.id
        collection_id = db_model.collection_id
        item_links = ItemLinks(
            #collection_id=collection_id, item_id=item_id, base_url=base_url
            collection_id=collection_id, item_id=item_id, href_builder=hrefbuilder
        ).create_links()

        token_param = {"token": hrefbuilder.token} if hrefbuilder.token else {}
        cog_url = _add_query_params(db_model.data_path, token_param)
        tiler_params = {"url": db_model.data_path, **token_param}

        # We don't save the links in the database, but create them on the fly
        #db_links = db_model.links
        add_links = [
            {
                "rel": "license",
                "href": "https://dataforsyningen.dk/Vilkaar",
                "type": "text/html; charset=UTF-8",
                "title": "SDFI license terms",
            },
            {
                "rel": "alternate",
                "href": _add_query_params(
                    f"{settings.cogtiler_basepath}/viewer.html", tiler_params
                ),
                "type": "text/html; charset=UTF-8",
                "title": "Interactive image viewer",
            },
        ]

        # We don't save the links in the database, but create them on the fly
        # if add_links:
        #     item_links += resolve_links(add_links, base_url)

        item_links += add_links

        # We don't save the stac_extensions in the database, but add them on the fly
        # stac_extensions = db_model.stac_extensions or []
        stac_extensions = [
            "https://stac-extensions.github.io/view/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json",
            "https://raw.githubusercontent.com/stac-extensions/perspective-imagery/main/json-schema/schema.json",  # TODO: Change when published...
        ]

        assets = {
            "data": {
                "href": cog_url,
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "title": "Raw tiff file",
            },
            "thumbnail": {
                "href": _add_query_params(
                    f"{settings.cogtiler_basepath}/thumbnail.jpg", tiler_params
                ),
                "type": "image/jpeg",
                "roles": ["thumbnail"],
                "title": "Thumbnail",
            },
        }

        # The custom geometry we are using emits geojson if the geometry is bound to the database
        # Otherwise it will return a geoalchemy2 WKBElement
        # TODO: It's probably best to just remove the custom geometry type
        # Geometry is named footprint in the database
        #geometry = db_model.geometry
        geometry = db_model.footprint
        if isinstance(geometry, ga.elements.WKBElement):
            geometry = ga.shape.to_shape(geometry).__geo_interface__
        if isinstance(geometry, str):
            geometry = json.loads(geometry)

        bbox = db_model.bbox
        if bbox is not None:
            bbox = [float(x) for x in db_model.bbox]

        # Properties

        # General props
        instrument_id = db_model.camera_id  # Used here and for pers:cam_id
        cls._add_if_not_none(properties, "gsd", db_model.gsd)
        properties["license"] = "various"
        cls._add_if_not_none(properties, "platform", "Fixed-wing aircraft")
        cls._add_if_not_none(properties, "instruments", [instrument_id])
        properties["providers"] = [
            {"name": db_model.producer, "roles": ["producer"]},
            {"url": "https://sdfi.dk/", "name": "SDFI", "roles": ["licensor", "host"]},
        ]    

        # Proj: https://github.com/stac-extensions/projection
        properties["proj:epsg"] = None
        properties["proj:shape"] = [
            db_model.sensor_rows,
            db_model.sensor_columns,
        ]  # Number of pixels in Y and X directions

        # View: https://github.com/stac-extensions/view
        cls._add_if_not_none(properties, "view:azimuth", db_model.azimuth)
        cls._add_if_not_none(properties, "view:off_nadir", db_model.offnadir)

        # Homegrown sdfi
        properties["direction"] = db_model.direction
        properties["estimated_accuracy"] = db_model.estacc

        # Perspective
        properties["pers:omega"] = db_model.omega
        properties["pers:phi"] = db_model.phi
        properties["pers:kappa"] = db_model.kappa
        properties["pers:perspective_center"] = [
            db_model.easting,
            db_model.northing,
            db_model.height,
        ]
        properties["pers:crs"] = db_model.horisontal_crs
        properties["pers:vertical_crs"] = db_model.vertical_crs
        properties["pers:rotation_matrix"] = db_model.rotmatrix

        properties["pers:interior_orientation"] = {
            "camera_id": instrument_id,
            "focal_length": db_model.focal_length,
            "pixel_spacing": [db_model.sensor_pixel_size, db_model.sensor_pixel_size],
            "calibration_date": db_model.calibration_date.strftime(DATE_RFC339)
            if db_model.calibration_date
            else None,
            "principal_point_offset": [
                db_model.principal_point_x,
                db_model.principal_point_y,
            ],  # Principal point offset in mm as [offset_x, offset_y]
            "sensor_array_dimensions": [
                db_model.sensor_columns,
                db_model.sensor_rows,
            ],  # Sensor dimensions as [number_of_columns, number_of_rows]
        }

        # Simple OGC API Features clients do not support "assets". Copy most important to the properties collection
        for copy_asset in ["data", "thumbnail"]:
            properties[f"asset:{copy_asset}"] = assets[copy_asset]["href"]

        return stac_types.Item(
            type="Feature",
            #stac_version=db_model.stac_version,
            stac_version="1.0.0",
            stac_extensions=stac_extensions,
            id=db_model.id,
            collection=db_model.collection_id,
            geometry=geometry,
            bbox=bbox,
            properties=properties,
            links=item_links,
            #assets=db_model.assets,
            assets=assets,
        )

    @classmethod
    def stac_to_db(
        cls, stac_data: TypedDict, exclude_geometry: bool = False
    ) -> database.Item:
        """Transform stac item to database model."""
        indexed_fields = {}
        for field in Settings.get().indexed_fields:
            # Use getattr to accommodate extension namespaces
            field_value = stac_data["properties"][field]
            if field == "datetime":
                field_value = rfc3339_str_to_datetime(field_value)
            indexed_fields[field.split(":")[-1]] = field_value

            # TODO: Exclude indexed fields from the properties jsonb field to prevent duplication

            now = now_to_rfc3339_str()
            if "created" not in stac_data["properties"]:
                stac_data["properties"]["created"] = now
            stac_data["properties"]["updated"] = now

        geometry = stac_data["geometry"]
        if geometry is not None:
            geometry = json.dumps(geometry)

        return database.Item(
            id=stac_data["id"],
            collection_id=stac_data["collection"],
            stac_version=stac_data["stac_version"],
            stac_extensions=stac_data.get("stac_extensions"),
            geometry=geometry,
            bbox=stac_data.get("bbox"),
            properties=stac_data["properties"],
            assets=stac_data["assets"],
            **indexed_fields,
        )


class CollectionSerializer(Serializer):
    """Serialization methods for STAC collections."""

    @classmethod
    #def db_to_stac(cls, db_model: database.Collection, base_url: str) -> TypedDict:
    def db_to_stac(cls, db_model: database.Collection, hrefbuilder: BaseHrefBuilder) -> TypedDict:
        """Transform database model to stac collection."""
        collection_links = CollectionLinks(
            #collection_id=db_model.id, base_url=base_url
            collection_id=db_model.id, href_builder=hrefbuilder
        ).create_links()

        db_links = db_model.links
        if db_links:
            #collection_links += resolve_links(db_links, base_url)
            collection_links += resolve_links(db_links, hrefbuilder.base_url)

        collection = stac_types.Collection(
            type="Collection",
            id=db_model.id,
            stac_version=db_model.stac_version,
            description=db_model.description,
            license=db_model.license,
            extent=db_model.extent,
            links=collection_links,
        )
        # We need to manually include optional values to ensure they are
        # excluded if we're not using response models.
        if db_model.stac_extensions:
            collection["stac_extensions"] = db_model.stac_extensions
        if db_model.title:
            collection["title"] = db_model.title
        if db_model.keywords:
            collection["keywords"] = db_model.keywords
        if db_model.providers:
            collection["providers"] = db_model.providers
        if db_model.summaries:
            collection["summaries"] = db_model.summaries
        return collection

    @classmethod
    def stac_to_db(
        cls, stac_data: TypedDict, exclude_geometry: bool = False
    ) -> database.Collection:
        """Transform stac collection to database model."""
        return database.Collection(**dict(stac_data))
