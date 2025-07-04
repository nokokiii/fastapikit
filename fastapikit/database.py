import functools
from typing import (
    AsyncGenerator,
    Callable, 
    List, 
    Optional, 
    Type, 
    TypeVar, 
    Union
)

from surrealdb import AsyncSurreal
from pydantic import BaseModel, ValidationError


########################
#####  Exceptions  #####
########################

class ResponseParasingError(Exception):
    """
    Custom exception for response parsing errors.
    """
    pass

class AuthError(Exception):
    """
    Custom exception for SurrealDB authentication errors.
    """
    pass


class QueryError(Exception):
    """
    Custom exception for database query errors.
    """
    pass


########################
######  Driver  ########
########################


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


########################
#####  Provider  #######
########################


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


########################
#####  Helpers  ########
########################


def simplify_record_ids(data):
    """
    Removes table name from the record id leaving only identifier.

    This is useful mostly for UUID identifiers.
    """
    if isinstance(data, list):
        return [simplify_record_ids(item) for item in data]
    
    elif isinstance(data, dict):
        return {k: simplify_record_ids(v) for k, v in data.items()}
    
    elif isinstance(data, BaseModel):
        return simplify_record_ids(data.model_dump())
    
    elif hasattr(data, "table_name") and hasattr(data, "id") and isinstance(data.id, str):
        return data.id
    else:   
        return data


def handle_db_results(_func: Union[Callable, None] = None, *, serialize: bool = True) -> Callable:
    """
    Decorator to handle database results.
    Can be used with or without parentheses:
    - @handle_db_results
    - @handle_db_results(serialize=False)
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                response = await func(*args, **kwargs)
                if response and serialize:
                    return simplify_record_ids(response)
                return response
            except Exception as e:
                raise QueryError("Database error occurred") from e
        return wrapper

    if callable(_func):
        return decorator(_func)  # Used as @handle_db_results
    return decorator  


ModelType = TypeVar("ModelType", bound=BaseModel)

def parse_list(model_class: Type[ModelType], data: list) -> List[ModelType]:
    """
    Parses a list of records into a list of models.
    """
    parsed_data = []
    for item in data:
        try:
            parsed_data.append(model_class(**item))
        except ValidationError as e:
            raise ResponseParasingError(model_class.__name__, str(e)) from e
    
    return parsed_data

