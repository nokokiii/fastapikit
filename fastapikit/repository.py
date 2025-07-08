from typing import Callable, Type, TypeVar

from fastapi import Depends

from .database import Driver, handle_db_results


class Repository:
    """
    Base repository class that provides a connection to the database.
    """
    def __init__(self, conn: Driver):
        if not conn or not hasattr(conn, "db"):
            raise ValueError("Invalid Driver instance passed to repository.")
        self.db = conn.db

    @handle_db_results(serialize=False)
    async def begin_transaction(self) -> None:
        """
        Function to begin transaction
        """
        await self.db.query("BEGIN TRANSACTION")
    
    @handle_db_results(serialize=False)
    async def commit_transcation(self) -> None:
        """
        Function to commit transaction
        """
        await self.db.query("COMMIT TRANSACTION")
    
    @handle_db_results(serialize=False)
    async def cancel_transaction(self) -> None:
        """
        Function to cancel transaction
        """
        await self.db.query("CANCEL TRANSACTION")


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
