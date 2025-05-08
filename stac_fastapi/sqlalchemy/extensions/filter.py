from enum import auto
from stac_pydantic.utils import AutoValueEnum

class BaseQueryables(str, AutoValueEnum):
    """Queryable fields.

    Define an enum of queryable fields and their data type.  Queryable fields are explicitly defined for two reasons:
        1. So the caller knows which fields they can query by
        2. Because JSONB queries with sqlalchemy ORM require casting the type of the field at runtime
            (see ``QueryableTypes``)
    """

    id = auto()
    collection_id = "collection"
    footprint = "geometry"
    datetime = auto()


class SkraafotosProperties(str, AutoValueEnum):
    easting = "pers:perspective_center.x"
    northing = "pers:perspective_center.y"
    height = "pers:perspective_center.z"
    vertical_crs = "pers:vertical_crs"
    horisontal_crs = "pers:crs"
    omega = "pers:omega"
    phi = "pers:phi"
    kappa = "pers:kappa"
    direction = auto()
    azimuth = "view:azimuth"
    offnadir = "view:off_nadir"
    estacc = "estimated_accuracy"
    producer = "providers.producer"
    gsd = auto()
    camera_id = "instruments"
    focal_length = "pers:interior_orientation.focal_length"
    calibration_date = "pers:interior_orientation.calibration_date"


class QueryableTypes:
    id = (
        None,
        "string",
        "ID",
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/id",
    )
    collection_id = (
        None,
        "string",
        "Collection ID",
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/collection",
    )
    footprint = (
        None,
        "Geometry",
        "Geometry",
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/geometry",
    )
    datetime = (
        None,
        "string",
        "Datetime",
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/datetime",
    )
    easting = (
        None,
        "number",
        "Easting",
        None,
    )
    northing = (
        None,
        "number",
        "Northing",
        None,
    )
    height = (
        None,
        "number",
        "Height",
        None,
    )
    vertical_crs = (
        None,
        "number",
        "Perspective vertical_crs",
        None,
    )
    horisontal_crs = (
        None,
        "number",
        "Perspective crs",
        None,
    )
    omega = (None, "number", "Perspective omega", None)
    phi = (None, "number", "Perspective phi", None)
    kappa = (None, "number", "Perspective kappa", None)
    direction = (None, "string", "Direction", None)
    azimuth = (None, "number", "View azimuth", None)
    offnadir = (None, "number", "View off_nadir", None)
    estacc = (None, "number", "Estimated accuracy", None)
    producer = (
        None,
        "string",
        "(Providers) Producer name",
        None,
    )
    gsd = (
        None,
        "number",
        "Ground Sample Distance",
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/instrument.json#/properties/gsd",
    )
    camera_id = (
        None,
        "string",
        "Instruments",
        "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/instrument.json#/properties/platform",
    )
    focal_length = (
        None,
        "string",
        "Perspective (Interior Orientation) Focal Length",
        None,
    )
    calibration_date = (
        None,
        "string",
        "Perspective (Interior Orientation) Calibration Date",
        None,
    )
