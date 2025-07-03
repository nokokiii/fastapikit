from typing import Callable, Generic, Type, TypeVar

from fastapi import Depends

from .repository import Repository, RepoType


class Service(Generic[RepoType]):
    def __init__(self, repo: RepoType):
        self.repo = repo


ServiceType = TypeVar('ServiceType', bound=Service)

def make_service_provider(
        service_cls: Type[ServiceType], 
        repo_provider: Callable[[], RepoType],
    ) -> Callable[..., ServiceType]:
    """
    Returns a FastAPI dependency that provides the given service class.
    """
    def provider(repo: Repository = Depends(repo_provider)) -> ServiceType:
        return service_cls(repo)
    
    return provider
