import os


class ConfigProvider:

    def default_catalog(self) -> str:
        return "p1pcat_prospect"

    def get_catalog(self) -> str:
        return os.environ.get("CATALOG", self.default_catalog())
    
    def get_mode(self) -> str:
        return os.environ.get("MODE", "dev")