from mindsdb.integrations.libs.const import HANDLER_TYPE

from .__about__ import __version__ as version, __description__ as description
from .connection_args import connection_args, connection_args_example

try:
    from .dune_handler import DuneHandler as Handler
    import_error = None
except Exception as e:
    Handler = None
    import_error = e

title = "Sim by Dune"
name = "dune"
type = HANDLER_TYPE.DATA
icon_path = "icon.svg"

# Enhanced metadata for MindsDB documentation system
permanent = True
author = "XplainCrypto Platform"
github_url = "https://github.com/xplaincrypto/mindsdb-handlers"
docs_url = "https://docs.sim.dune.com/"

# Authentication requirements
requires_api_key = True
auth_type = "api_key"

# Handler capabilities
supports_select = True
supports_insert = False
supports_update = False
supports_delete = False

# Categories and tags for discovery
category = "blockchain"
keywords = ["blockchain", "ethereum", "wallet_analytics", "transactions", "nft", "defi", "evm"]

__all__ = [
    "Handler", "version", "name", "type", "title", "description",
    "connection_args", "connection_args_example", "import_error", "icon_path",
    "permanent", "author", "github_url", "docs_url", "requires_api_key",
    "auth_type", "supports_select", "category", "keywords"
] 