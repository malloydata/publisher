"""A client library for accessing Malloy Publisher - Semantic Model Serving API"""

from .client import AuthenticatedClient, Client

__all__ = (
    "AuthenticatedClient",
    "Client",
)
