import abc
from utils.error_handling import SecretManagerError


class BaseSecretManager(abc.ABC):
    @abc.abstractmethod
    def get_secret(self, secret_name: str) -> dict:
        pass


class AWSSecretManager(BaseSecretManager):
    def __init__(self):
        try:
            import boto3
            from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
        except ImportError as e:
            raise SecretManagerError("Missing AWS dependencies") from e

        self.client = boto3.client('secretsmanager')
        self.cache = SecretCache(SecretCacheConfig())

    def get_secret(self, secret_name: str) -> dict:
        try:
            return self.cache.get_secret_string(secret_name)
        except Exception as e:
            raise SecretManagerError(f"Failed to retrieve secret {secret_name}: {str(e)}") from e


class VaultSecretManager(BaseSecretManager):
    def __init__(self, vault_addr: str):
        try:
            import hvac
        except ImportError as e:
            raise SecretManagerError("Missing Vault dependencies") from e

        self.client = hvac.Client(url=vault_addr)

    def get_secret(self, secret_name: str) -> dict:
        try:
            response = self.client.read_secret_version(path=secret_name)
            return response['data']['data']
        except Exception as e:
            raise SecretManagerError(f"Vault secret error: {str(e)}") from e


def get_secret_manager(provider: str = 'aws', **kwargs) -> BaseSecretManager:
    try:
        if provider == 'aws':
            return AWSSecretManager()
        elif provider == 'vault':
            return VaultSecretManager(kwargs['vault_addr'])
        raise SecretManagerError(f"Unsupported provider: {provider}")
    except KeyError as e:
        raise SecretManagerError(f"Missing configuration for {provider}") from e