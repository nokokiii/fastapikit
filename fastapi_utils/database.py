from typing import AsyncGenerator, Callable, Optional
from surrealdb import AsyncSurreal


class AuthError(Exception):
    """Custom exception for SurrealDB authentication errors."""
    pass


class Driver:
    """
    SurrealDB driver that creates a new connection on instantiation.
    """
    def __init__(
        self,
        host: str,
        ns: str,
        db: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None
    ) -> None:
        if not token and (user is None or password is None):
            raise AuthError("Provide either a token or both username and password.")

        self.db = AsyncSurreal(host)
        self.namespace = ns
        self.database = db
        self.user = user
        self.password = password
        self.token = token
        self.connected = False

    async def _login(self) -> None:
        """
        Authenticates the user with SurrealDB.
        """
        try:
            if self.token:
                await self.db.authenticate(token=self.token)
            else:
                await self.db.signin({"username": self.user, "password": self.password})
        except Exception as e:
            raise AuthError(f"Authentication failed: {e}")

    async def connect(self) -> None:
        """
        Connects to the database and sets namespace/database context.
        """
        await self.db.connect()
        await self._login()
        await self.db.use(namespace=self.namespace, database=self.database)
        self.connected = True

    async def close(self) -> None:
        """
        Closes the connection if open.
        """
        if self.connected:
            await self.db.close()
            self.connected = False


def make_driver_provider(
    host: str,
    ns: str,
    db: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None
) -> Callable[[], AsyncGenerator[Driver, None]]:
    """
    Factory function for creating a Driver instance.

    Example for FastAPI dependency:
    
    `get_driver = make_driver_provider(...)`
    """
    async def provider() -> AsyncGenerator[Driver, None]:
        driver = Driver(host=host, ns=ns, db=db, user=user, password=password, token=token)
        await driver.connect()
        try:
            yield driver
        finally:
            await driver.close()

    return provider
