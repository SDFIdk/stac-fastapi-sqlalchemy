"""Item crud client."""
import json
import logging
import operator
from datetime import datetime
from typing import List, Optional, Set, Type, Union, Dict, Any
from urllib.parse import unquote_plus, urlencode, urljoin

import attr
import geoalchemy2 as ga
import sqlalchemy as sa
import stac_pydantic
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry import shape
from sqlakeyset import get_page
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import array
from sqlalchemy.orm import Session as SqlSession, with_expression
from stac_fastapi.types.config import Settings
from stac_fastapi.types.core import BaseCoreClient, BaseFiltersClient
from stac_fastapi.types.errors import NotFoundError
from stac_fastapi.types.links import BaseHrefBuilder
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Collection, Collections, Item, ItemCollection
from stac_pydantic.links import Relations
from stac_pydantic.shared import MimeTypes

from stac_fastapi.sqlalchemy import serializers
from stac_fastapi.sqlalchemy.extensions.filter import QueryableTypes
from stac_fastapi.sqlalchemy.extensions.query import Operator
from stac_fastapi.sqlalchemy.models import database
from stac_fastapi.sqlalchemy.session import Session
from stac_fastapi.sqlalchemy.tokens import PaginationTokenClient
from stac_fastapi.sqlalchemy.types.filter import Queryables
from stac_fastapi.sqlalchemy.types.links import ApiTokenHrefBuilder

logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CoreCrudClient(PaginationTokenClient, BaseCoreClient):
    """Client for core endpoints defined by stac."""

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    item_table: Type[database.Item] = attr.ib(default=database.Item)
    collection_table: Type[database.Collection] = attr.ib(default=database.Collection)
    item_serializer: Type[serializers.Serializer] = attr.ib(
        default=serializers.ItemSerializer
    )
    collection_serializer: Type[serializers.Serializer] = attr.ib(
        default=serializers.CollectionSerializer
    )

    @staticmethod
    def _lookup_id(
        id: str, table: Type[database.BaseModel], session: SqlSession
    ) -> Type[database.BaseModel]:
        """Lookup row by id."""
        row = session.query(table).filter(table.id == id).first()
        if not row:
            raise NotFoundError(f"{table.__name__} {id} not found")
        return row
    

    def _bbox_expression(self):
    #def _bbox_expression(self, to_srid: int):
        """Returns Ad Hoc SQL expression which can be applied to a "deferred expression" attribute.
        We don't have bbox as a column in the database, but we imitate with query_expression() and with_expression().
        with_expression() needs to be triggered for it to be made Ad Hoc
        The expression makes sure the BBOX is returned in the requested SRID."""
        # if to_srid != self.storage_srid:
        #     geom = ga.func.ST_Transform(self.item_table.footprint, to_srid)
        # else:
        #     geom = self.item_table.footprint
        geom = self.item_table.footprint

        return with_expression(
            self.item_table.bbox,
            array(
                [
                    ga.func.ST_XMin(ga.func.ST_Envelope(geom)),
                    ga.func.ST_YMin(ga.func.ST_Envelope(geom)),
                    ga.func.ST_XMax(ga.func.ST_Envelope(geom)),
                    ga.func.ST_YMax(ga.func.ST_Envelope(geom)),
                ]
            ),
        )

    def create_crs_response(self, resp, crs, **kwargs) -> JSONResponse:
        """Add Content-Crs header to JSONResponse to comply with OGC API Feat part 2"""
        crs_ext = self.get_extension("CrsExtension")
        if crs is None:
            crs = crs_ext.storageCrs
        if crs in crs_ext.crs:  # If the CRS is valid
            return JSONResponse(resp, headers={"Content-Crs": crs})
        else:
            return resp

    def href_builder(self, **kwargs):
        """Override with HrefBuilder which adds API token to all hrefs if present"""
        request = kwargs["request"]
        base_url = str(request.base_url)
        token = request.query_params.get("token")
        # return BaseHrefBuilder(base_url, token)
        return ApiTokenHrefBuilder(base_url, token)

    def all_collections(self, **kwargs) -> Collections:
        """Read all collections from the database."""
        #base_url = str(kwargs["request"].base_url)
        hrefbuilder = self.href_builder(**kwargs)
        with self.session.reader.context_session() as session:
            collections = session.query(self.collection_table).all()
            serialized_collections = [
                #self.collection_serializer.db_to_stac(collection, base_url=base_url)
                self.collection_serializer.db_to_stac(
                    collection, hrefbuilder=hrefbuilder)
                for collection in collections
            ]

            if self.extension_is_enabled("CrsExtension"):
                for c in serialized_collections:
                    c.update({"crs": self.get_extension("CrsExtension").crs})

            links = [
                {
                    "rel": Relations.root.value,
                    "type": MimeTypes.json,
                    #"href": base_url,
                    "href": hrefbuilder.build("./"),
                },
                {
                    "rel": Relations.parent.value,
                    "type": MimeTypes.json,
                    #"href": base_url,
                    "href": hrefbuilder.build("./"),
                },
                {
                    "rel": Relations.self.value,
                    "type": MimeTypes.json,
                    #"href": urljoin(base_url, "collections"),
                    "href": hrefbuilder.build("./collections"),
                },
            ]
            collection_list = Collections(
                collections=serialized_collections or [], links=links
            )
            return collection_list


    def get_collection(self, collection_id: str, **kwargs) -> Collection:
        """Get collection by id."""
        #base_url = str(kwargs["request"].base_url)
        hrefbuilder = self.href_builder(**kwargs)
        with self.session.reader.context_session() as session:
            collection = self._lookup_id(collection_id, self.collection_table, session)

            # return self.collection_serializer.db_to_stac(collection, base_url)
            serialized_collection = self.collection_serializer.db_to_stac(
                collection, hrefbuilder)

            # Add the list of service supported CRS to the collection
            if self.extension_is_enabled("CrsExtension"):
                serialized_collection.update(
                    {"crs": self.get_extension("CrsExtension").crs}
                )

            return serialized_collection

    def item_collection(
        self,
        collection_id: str,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[str] = None,
        crs: Optional[str] = None,
        limit: int = 10,
        #token: str = None,
        pt: str = None,
        **kwargs,
    ) -> ItemCollection:
        """Read an item collection from the database."""
        # base_url = str(kwargs["request"].base_url)
        hrefbuilder = self.href_builder(**kwargs)
        with self.session.reader.context_session() as session:
            # Look up the collection first to get a 404 if it doesn't exist
            _ = self._lookup_id(collection_id, self.collection_table, session)
            query = (
                session.query(self.item_table)
                .join(self.collection_table)
                .filter(self.collection_table.id == collection_id)
                .order_by(self.item_table.datetime.desc(), self.item_table.id)
            )
            query = query.options(self._bbox_expression())
            # Spatial query
            geom = None
            if bbox:
                bbox = [float(x) for x in bbox]
                if len(bbox) == 4:
                    geom = ShapelyPolygon.from_bounds(*bbox)
                elif len(bbox) == 6:
                    """Shapely doesn't support 3d bounding boxes so use the 2d portion"""
                    bbox_2d = [bbox[0], bbox[1], bbox[3], bbox[4]]
                    geom = ShapelyPolygon.from_bounds(*bbox_2d)
            if geom:
                filter_geom = ga.shape.from_shape(geom, srid=4326)
                query = query.filter(
                    ga.func.ST_Intersects(self.item_table.footprint, filter_geom)
                )

            # Temporal query
            if datetime:
                # Two tailed query (between)
                dts = datetime.split("/")
                # Non-interval date ex. "2000-02-02T00:00:00.00Z"
                if len(dts) == 1:
                    query = query.filter(self.item_table.datetime == dts[0])
                # is there a benefit to between instead of >= and <= ?
                elif dts[0] not in ["", ".."] and dts[1] not in ["", ".."]:
                    query = query.filter(self.item_table.datetime.between(*dts))
                # All items after the start date
                elif dts[0] not in ["", ".."]:
                    query = query.filter(self.item_table.datetime >= dts[0])
                # All items before the end date
                elif dts[1] not in ["", ".."]:
                    query = query.filter(self.item_table.datetime <= dts[1])

            if crs and self.extension_is_enabled("CrsExtension"):
                if not self.get_extension("CrsExtension").is_crs_supported(crs):
                    raise HTTPException(
                        status_code=400,
                        detail="CRS provided for argument crs is invalid, valid options are: "
                        + ",".join(self.get_extension("CrsExtension").crs),
                    )
                
            count = None
            if self.extension_is_enabled("ContextExtension"):
                count_query = query.statement.with_only_columns(
                    [func.count()]
                ).order_by(None)
                count = query.session.execute(count_query).scalar()
                
            #token = self.get_token(token) if token else token
            pagination_token = (self.from_token(pt) if pt else pt)
            #page = get_page(query, per_page=limit, page=(token or False))
            page = get_page(query, per_page=limit, page=(pagination_token or False))
            # Create dynamic attributes for each page
            page.next = (
                # We don't insert tokens into the database
                #self.insert_token(keyset=page.paging.bookmark_next)
                self.to_token(keyset=page.paging.bookmark_next)
                if page.paging.has_next
                else None
            )
            page.previous = (
                # We don't insert tokens into the database
                #self.insert_token(keyset=page.paging.bookmark_previous)
                self.to_token(keyset=page.paging.bookmark_previous)
                if page.paging.has_previous
                else None
            )

            links = [
                {
                    "rel": Relations.self.value,
                    "type": "application/geo+json",
                    "href": str(kwargs["request"].url),
                },
                {
                    "rel": Relations.root.value,
                    "type": "application/json",
                    "href": str(kwargs["request"].base_url),
                },
                {
                    "rel": Relations.parent.value,
                    "type": "application/json",
                    "href": str(kwargs["request"].base_url),
                },
            ]
            if page.next:
                links.append(
                    {
                        "rel": Relations.next.value,
                        "type": "application/geo+json",
                        #"href": f"{kwargs['request'].base_url}collections/{collection_id}/items?token={page.next}&limit={limit}",
                        "href": f"{kwargs['request'].base_url}collections/{collection_id}/items?pt={page.next}&limit={limit}",
                        "method": "GET",
                    }
                )
            if page.previous:
                links.append(
                    {
                        "rel": Relations.previous.value,
                        "type": "application/geo+json",
                        #"href": f"{kwargs['request'].base_url}collections/{collection_id}/items?token={page.previous}&limit={limit}",
                        "href": f"{kwargs['request'].base_url}collections/{collection_id}/items?pt={page.previous}&limit={limit}",
                        "method": "GET",
                    }
                )

            response_features = []
            for item in page:
                serialized_item = self.item_serializer.db_to_stac(
                    item, hrefbuilder=hrefbuilder)
                response_features.append(
                    # self.item_serializer.db_to_stac(item, base_url=base_url)
                    serialized_item
                )
                if self.extension_is_enabled("CrsExtension"):
                    if self.get_extension("CrsExtension"):
                        # If the CRS type has not been populated to the response
                        if ("crs" not in serialized_item["properties"]):
                            crs_obj = {
                                "type": "name",
                                "properties": {"name": f"{crs}"},
                            }
                            serialized_item["properties"]["crs"] = crs_obj
        
            context_obj = None
            if self.extension_is_enabled("ContextExtension"):
                context_obj = {
                    "returned": len(page),
                    "limit": limit,
                    "matched": count,
                }

            # The response has to be returned as a ItemCollection type
            resp = ItemCollection(
                type="FeatureCollection",
                features=response_features,
                links=links,
                context=context_obj,
            )

            # If the CRS extension is enable we return the response here with an content-crs header 
            if self.extension_is_enabled("CrsExtension"):
                return self.create_crs_response(resp, crs)

            # If the CRS extension is disable we return the reponse here
            return resp

    def get_item(self, item_id: str, collection_id: str, **kwargs) -> Item:
        """Get item by id."""
        request = kwargs["request"]
        req_crs = request.query_params.get("crs")
        if req_crs and self.extension_is_enabled("CrsExtension"):
            stac_crs = self.get_extension("CrsExtension")
            if self.get_extension("CrsExtension").is_crs_supported(req_crs):
                output_srid = stac_crs.epsg_from_crs(req_crs)
            else:
                raise HTTPException(
                    status_code=400,
                    detail="CRS provided for argument crs is invalid, valid options are: "
                    + ",".join(self.get_extension("CrsExtension").crs),
                )
        else:
            req_crs = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
        # base_url = str(kwargs["request"].base_url)
        hrefbuilder = self.href_builder(**kwargs)
        with self.session.reader.context_session() as session:
            db_query = session.query(self.item_table)
            db_query = db_query.filter(self.item_table.collection_id == collection_id)
            db_query = db_query.filter(self.item_table.id == item_id)
            db_query = db_query.options(self._bbox_expression())
            item = db_query.first()
            if not item:
                raise NotFoundError(f"{self.item_table.__name__} {item_id} not found")
            # return self.item_serializer.db_to_stac(item, base_url=base_url)
            resp = self.item_serializer.db_to_stac(
                item, hrefbuilder=hrefbuilder)

            if self.get_extension("CrsExtension"):
                if (
                    "crs" not in resp["properties"]
                ):  # If the CRS type has not been populated to the response
                    crs_obj = {
                        "type": "name",
                        "properties": {"name": f"{req_crs}"},
                    }
                resp["properties"]["crs"] = crs_obj
                return self.create_crs_response(resp, req_crs)

            return resp
        
    def get_search(
        self,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        #token: Optional[str] = None,
        pt: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        intersects: Optional[str] = None,
        **kwargs,
    ) -> ItemCollection:
        """GET search catalog."""
        # Parse request parameters
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            #"token": token,
            "pt": pt,
            "query": json.loads(unquote_plus(query)) if query else query,
        }

        if datetime:
            base_args["datetime"] = datetime

        if intersects:
            base_args["intersects"] = json.loads(unquote_plus(intersects))

        if sortby:
            # https://github.com/radiantearth/stac-spec/tree/master/api-spec/extensions/sort#http-get-or-post-form
            sort_param = []
            for sort in sortby:
                sort_param.append(
                    {
                        "field": sort[1:],
                        "direction": "asc" if sort[0] == "+" else "desc",
                    }
                )
            base_args["sortby"] = sort_param

        if fields:
            includes = set()
            excludes = set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                elif field[0] == "+":
                    includes.add(field[1:])
                else:
                    includes.add(field)
            base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError:
            raise HTTPException(status_code=400, detail="Invalid parameters provided")
        resp = self.post_search(search_request, request=kwargs["request"])

        # Pagination
        page_links = []
        for link in resp["links"]:
            if link["rel"] == Relations.next or link["rel"] == Relations.previous:
                query_params = dict(kwargs["request"].query_params)
                if link["body"] and link["merge"]:
                    query_params.update(link["body"])
                link["method"] = "GET"
                link["href"] = f"{link['href']}?{urlencode(query_params)}"
                link["body"] = None
                link["merge"] = False
                page_links.append(link)
            else:
                page_links.append(link)
        resp["links"] = page_links
        return resp

    def post_search(
        self, search_request: BaseSearchPostRequest, **kwargs
    ) -> ItemCollection:
        """POST search catalog."""
        #base_url = str(kwargs["request"].base_url)
        hrefbuilder = self.href_builder(**kwargs)
        
        with self.session.reader.context_session() as session:
            # We create paginating tokens on the fly
            # token = (
            #     self.get_token(search_request.token) if search_request.token else False
            # )
            pagination_token = (
                self.from_token(search_request.pt) if search_request.pt else False
            )
            query = session.query(self.item_table)

            #query = query.options(self._bbox_expression(output_srid))
            query = query.options(self._bbox_expression())

            # Filter by collection
            count = None
            if search_request.collections:
                query = query.join(self.collection_table).filter(
                    sa.or_(
                        *[
                            self.collection_table.id == col_id
                            for col_id in search_request.collections
                        ]
                    )
                )

            # Sort
            if search_request.sortby:
                sort_fields = [
                    getattr(
                        self.item_table.get_field(sort.field),
                        sort.direction.value,
                    )()
                    for sort in search_request.sortby
                ]
                sort_fields.append(self.item_table.id)
                query = query.order_by(*sort_fields)
            else:
                # Default sort is date
                query = query.order_by(
                    self.item_table.datetime.desc(), self.item_table.id
                )

            # Ignore other parameters if ID is present
            if search_request.ids:
                id_filter = sa.or_(
                    *[self.item_table.id == i for i in search_request.ids]
                )
                items = query.filter(id_filter).order_by(self.item_table.id)
                #page = get_page(items, per_page=search_request.limit, page=token)
                page = get_page(items, per_page=search_request.limit, page=pagination_token)
                if self.extension_is_enabled("ContextExtension"):
                    count = len(search_request.ids)
                page.next = (
                    # We don't insert tokens into the database
                    #self.insert_token(keyset=page.paging.bookmark_next)
                    self.to_token(keyset=page.paging.bookmark_next)
                    if page.paging.has_next
                    else None
                )
                page.previous = (
                    # We don't insert tokens into the database
                    #self.insert_token(keyset=page.paging.bookmark_previous)
                    self.to_token(keyset=page.paging.bookmark_previous)
                    if page.paging.has_previous
                    else None
                )

            else:
                # Spatial query
                geom = None
                if search_request.intersects is not None:
                    geom = shape(search_request.intersects)
                elif search_request.bbox:
                    if len(search_request.bbox) == 4:
                        geom = ShapelyPolygon.from_bounds(*search_request.bbox)
                    elif len(search_request.bbox) == 6:
                        """Shapely doesn't support 3d bounding boxes we'll just use the 2d portion"""
                        bbox_2d = [
                            search_request.bbox[0],
                            search_request.bbox[1],
                            search_request.bbox[3],
                            search_request.bbox[4],
                        ]
                        geom = ShapelyPolygon.from_bounds(*bbox_2d)

                if geom:
                    filter_geom = ga.shape.from_shape(geom, srid=4326)
                    query = query.filter(
                        ga.func.ST_Intersects(self.item_table.geometry, filter_geom)
                    )

                # Temporal query
                if search_request.datetime:
                    # Two tailed query (between)
                    dts = search_request.datetime.split("/")
                    # Non-interval date ex. "2000-02-02T00:00:00.00Z"
                    if len(dts) == 1:
                        query = query.filter(self.item_table.datetime == dts[0])
                    # is there a benefit to between instead of >= and <= ?
                    elif dts[0] not in ["", ".."] and dts[1] not in ["", ".."]:
                        query = query.filter(self.item_table.datetime.between(*dts))
                    # All items after the start date
                    elif dts[0] not in ["", ".."]:
                        query = query.filter(self.item_table.datetime >= dts[0])
                    # All items before the end date
                    elif dts[1] not in ["", ".."]:
                        query = query.filter(self.item_table.datetime <= dts[1])

                # We don't support query parameter `query`
                # Query fields
                # if search_request.query:
                #     for field_name, expr in search_request.query.items():
                #         field = self.item_table.get_field(field_name)
                #         for op, value in expr.items():
                #             if op == Operator.gte:
                #                 query = query.filter(operator.ge(field, value))
                #             elif op == Operator.lte:
                #                 query = query.filter(operator.le(field, value))
                #             else:
                #                 query = query.filter(op.operator(field, value))

                if self.extension_is_enabled("ContextExtension"):
                    count_query = query.statement.with_only_columns(
                        [func.count()]
                    ).order_by(None)
                    count = query.session.execute(count_query).scalar()
                #page = get_page(query, per_page=search_request.limit, page=token)
                page = get_page(query, per_page=search_request.limit, page=pagination_token)
                # Create dynamic attributes for each page
                page.next = (
                    # We don't insert tokens into the database
                    #self.insert_token(keyset=page.paging.bookmark_next)
                    self.to_token(keyset=page.paging.bookmark_next)
                    if page.paging.has_next
                    else None
                )
                page.previous = (
                    # We don't insert tokens into the database
                    #self.insert_token(keyset=page.paging.bookmark_previous)
                    self.to_token(keyset=page.paging.bookmark_previous)
                    if page.paging.has_previous
                    else None
                )

            links = []
            if page.next:
                links.append(
                    {
                        "rel": Relations.next.value,
                        "type": "application/geo+json",
                        "href": f"{kwargs['request'].base_url}search",
                        "method": "POST",
                        #"body": {"token": page.next},
                        "body": {"pt": page.next},
                        "merge": True,
                    }
                )
            if page.previous:
                links.append(
                    {
                        "rel": Relations.previous.value,
                        "type": "application/geo+json",
                        "href": f"{kwargs['request'].base_url}search",
                        "method": "POST",
                        #"body": {"token": page.previous},
                        "body": {"pt": page.previous},
                        "merge": True,
                    }
                )

            response_features = []
            filter_kwargs = {}

            for item in page:
                response_features.append(
                    #self.item_serializer.db_to_stac(item, base_url=base_url)
                    self.item_serializer.db_to_stac(item, hrefbuilder=hrefbuilder)
                )

            # Use pydantic includes/excludes syntax to implement fields extension
            if self.extension_is_enabled("FieldsExtension"):
                if search_request.query is not None:
                    query_include: Set[str] = set(
                        [
                            k
                            if k in Settings.get().indexed_fields
                            else f"properties.{k}"
                            for k in search_request.query.keys()
                        ]
                    )
                    if not search_request.fields.include:
                        search_request.fields.include = query_include
                    else:
                        search_request.fields.include.union(query_include)

                filter_kwargs = search_request.fields.filter_fields
                # Need to pass through `.json()` for proper serialization
                # of datetime
                response_features = [
                    json.loads(stac_pydantic.Item(**feat).json(**filter_kwargs))
                    for feat in response_features
                ]

        context_obj = None
        if self.extension_is_enabled("ContextExtension"):
            context_obj = {
                "returned": len(page),
                "limit": search_request.limit,
                "matched": count,
            }

        return ItemCollection(
            type="FeatureCollection",
            features=response_features,
            links=links,
            context=context_obj,
        )


@attr.s
class CoreFiltersClient(BaseFiltersClient):
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    def validate_collection(self, value):
        # client = CoreCrudClient(session=self.session, collection_table=database.Collection)
        with self.session.reader.context_session() as session:
            try:
                CoreCrudClient._lookup_id(value, database.Collection, session)
            except:
                raise ValueError(f"Collection '{value}' doesn't exist")

    def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension

        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables
        """

        base_url = str(kwargs["request"].base_url)
        if "collection_id" in str(kwargs["request"].path_params):
            collection_id = str(kwargs["request"].path_params["collection_id"])
            try:
                self.validate_collection(collection_id)
            except ValueError as e:
                raise HTTPException(
                    status_code=404,
                    detail=["Not found"] + str(e).split("\n"),
                )

        # Check that collection exists

        base_queryables, queryables = (
            Queryables.get_queryable_properties_intersection([collection_id])
            if collection_id
            else Queryables.get_queryable_properties_intersection()
        )

        res = {}
        queryables.sort()
        for q in base_queryables + queryables:
            q_type = getattr(QueryableTypes, Queryables.get_queryable(q).name)
            res[q] = {
                "description": q_type[2],
                "$ref" if q_type[3] else "type": q_type[3] if q_type[3] else q_type[1],
            }

        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": urljoin(
                base_url,
                f"collections/{collection_id}/queryables"
                if collection_id
                else f"queryables",
            ),
            "type": "object",
            "title": f"{collection_id.capitalize() if collection_id else 'Dataforsyningen FlyfotoAPI - Shared queryables'}",
            "properties": res,
        }
