from typing import Callable, Type, TypeVar

from fastapi import Depends

from .database import Driver


class Repository:
    """
    Base repository class that provides a connection to the database.
    """
    def __init__(self, conn: Driver):
        if not conn or not hasattr(conn, "db"):
            raise ValueError("Invalid Driver instance passed to repository.")
        self.db = conn.db


RepoType = TypeVar('RepoType', bound='Repository')

def make_repo_provider(
        repo_cls: Type[RepoType], 
        driver_provider: Callable[[], Driver]
    ) -> Callable[..., RepoType]:
    """
    Returns a FastAPI dependency that provides the given repository class.
    """
    def provider(conn: Driver = Depends(driver_provider)) -> RepoType:
        return repo_cls(conn)
    return provider
