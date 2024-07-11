"""FastAPI application."""
import os

from typing import Optional
from fastapi import security
from fastapi.params import Depends
from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import create_get_request_model, create_post_request_model
from stac_fastapi.extensions.core import (
    ContextExtension,
    CrsExtension,
    #FieldsExtension,
    FilterExtension,
    SortExtension,
    TokenPaginationExtension,
    #TransactionExtension,
)
from stac_fastapi.extensions.third_party import BulkTransactionExtension

from stac_fastapi.sqlalchemy.config import SqlalchemySettings
from stac_fastapi.sqlalchemy.core import CoreCrudClient, CoreFiltersClient
from stac_fastapi.sqlalchemy.extensions import QueryExtension
from stac_fastapi.sqlalchemy.session import Session
from stac_fastapi.sqlalchemy.transactions import (
    BulkTransactionsClient,
    TransactionsClient,
)

def token_header_param(
    header_token: Optional[str] = Depends(
        security.api_key.APIKeyHeader(name="token", auto_error=False)
    ),
):
    """This defines an api-key header param named 'token'"""
    # Set auto_error to `True` to make `token `required.
    pass


def token_query_param(
    query_token: Optional[str] = Depends(
        security.api_key.APIKeyQuery(name="token", auto_error=False)
    ),
):
    """This defines an api-key query param named 'token'"""
    # Set auto_error to `True` to make `token `required.
    pass


# Here we add all paths which produces internal links as they must include the token
ROUTES_REQUIRING_TOKEN = [
    {"path": "/", "method": "GET"},
    {"path": "/conformance", "method": "GET"},
    {"path": "/search", "method": "GET"},
    {"path": "/search", "method": "POST"},
    {"path": "/collections", "method": "GET"},
    {"path": "/collections/{collectionId}", "method": "GET"},
    {"path": "/collections/{collectionId}/items", "method": "GET"},
    {"path": "/collections/{collectionId}/items/{itemId}", "method": "GET"},
    {"path": "/queryables", "method": "GET"},
    {"path": "/collections/{collectionId}/queryables", "method": "GET"},
    {"path": "/ping", "method": "GET"},
]

settings = SqlalchemySettings()
session = Session.create_from_settings(settings)
extensions = [
    #TransactionExtension(client=TransactionsClient(session=session), settings=settings),
    #BulkTransactionExtension(client=BulkTransactionsClient(session=session)),
    #FieldsExtension(),
    #QueryExtension(),
    FilterExtension(client=CoreFiltersClient(session=session)),
    SortExtension(),
    TokenPaginationExtension(),
    ContextExtension(),
    CrsExtension(),
]

post_request_model = create_post_request_model(extensions)

api = StacApi(
    # Override default title and description.
    title="Skråfoto STAC API",
    description="API til udstilling af metadata for flyfotos.",
    settings=settings,
    extensions=extensions,
    client=CoreCrudClient(
        session=session, extensions=extensions, post_request_model=post_request_model,
        # Override default landing_page_id
        landing_page_id="dataforsyningen-flyfotoapi",
    ),
    search_get_request_model=create_get_request_model(extensions),
    search_post_request_model=post_request_model,
    route_dependencies=[(ROUTES_REQUIRING_TOKEN, [Depends(token_header_param), Depends(token_query_param)])],
)
app = api.app


def run():
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        uvicorn.run(
            "stac_fastapi.sqlalchemy.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="info",
            reload=settings.reload,
            root_path=os.getenv("UVICORN_ROOT_PATH", ""),
        )
    except ImportError:
        raise RuntimeError("Uvicorn must be installed in order to use command")


if __name__ == "__main__":
    run()


def create_handler(app):
    """Create a handler to use with AWS Lambda if mangum available."""
    try:
        from mangum import Mangum

        return Mangum(app)
    except ImportError:
        return None


handler = create_handler(app)
