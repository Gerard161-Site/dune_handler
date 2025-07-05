import requests
from typing import Optional, Dict, Any
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql
from .dune_tables import (
    BalancesTable,
    TransactionsTable,
    CollectiblesTable,
    QueriesTable,
    ExecutionsTable,
    ResultsTable,
    ContractsTable,
    DEXTable,
    MarketsTable
)

logger = log.getLogger(__name__)


class DuneHandler(APIHandler):
    """
    The Dune Analytics handler implementation.
    """
    
    name = 'dune'
    
    def __init__(self, name: str, **kwargs):
        """
        Initialize the Dune Analytics handler.
        
        Args:
            name (str): The handler name
            kwargs: Connection arguments including api_key
        """
        super().__init__(name)
        
        # Connection parameters
        connection_data = kwargs.get('connection_data', {})
        self.api_key = connection_data.get('api_key')
        self.base_url = connection_data.get('base_url', 'https://api.sim.dune.com/v1')
        
        # API configuration
        self.headers = {
            'User-Agent': 'MindsDB-Dune-Handler/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        
        if self.api_key:
            self.headers['X-Sim-Api-Key'] = self.api_key
        
        # Register available tables
        self._register_table('balances', BalancesTable(self))
        self._register_table('transactions', TransactionsTable(self))
        self._register_table('collectibles', CollectiblesTable(self))
        self._register_table('queries', QueriesTable(self))
        self._register_table('executions', ExecutionsTable(self))
        self._register_table('results', ResultsTable(self))
        self._register_table('contracts', ContractsTable(self))
        self._register_table('dex', DEXTable(self))
        self._register_table('markets', MarketsTable(self))
        
    def connect(self) -> StatusResponse:
        """
        Set up any connections required by the handler.
        
        Returns:
            HandlerStatusResponse
        """
        try:
            # Simple connection test - just check if we have a valid API key
            if not self.api_key:
                self.is_connected = False
                return StatusResponse(False, "Connection failed: API key is required")
            
            # FIXED: Test connectivity with a known working endpoint
            # Use a test address to validate API key without needing real data
            test_address = "0x0000000000000000000000000000000000000000"  # Test with zero address
            test_url = f"{self.base_url}/evm/balances/{test_address}"
            
            response = requests.get(test_url, headers=self.headers, timeout=10)
            
            if response.status_code in [401, 403]:
                self.is_connected = False
                return StatusResponse(False, "Connection failed: Invalid API key")
            elif response.status_code in [200, 404]:
                # 200 or 404 means API key is valid (404 is fine for test address)
                self.is_connected = True
                return StatusResponse(True, "Connection established successfully")
            else:
                # Other status codes indicate potential issues
                self.is_connected = False
                return StatusResponse(False, f"Connection test returned status {response.status_code}")
                
        except requests.exceptions.Timeout:
            self.is_connected = False
            return StatusResponse(False, "Connection failed: Request timeout")
        except requests.exceptions.ConnectionError:
            self.is_connected = False  
            return StatusResponse(False, "Connection failed: Cannot connect to Sim by Dune API")
        except Exception as e:
            self.is_connected = False
            logger.error(f"Error connecting to Sim by Dune: {e}")
            return StatusResponse(False, f"Connection failed: {str(e)}")
    
    def check_connection(self) -> StatusResponse:
        """
        Check if the connection is alive and healthy.
        
        Returns:
            HandlerStatusResponse
        """
        return self.connect()
    
    def native_query(self, query: str) -> Response:
        """
        Receive and process a raw query.
        
        Args:
            query (str): query in native format
            
        Returns:
            HandlerResponse
        """
        ast = parse_sql(query, dialect='mindsdb')
        return self.query(ast)
    
    def call_dune_api(self, endpoint: str, params: Optional[Dict] = None, method: str = 'GET', data: Optional[Dict] = None) -> Any:
        """
        Call Dune Analytics API endpoint.
        
        Args:
            endpoint (str): API endpoint path
            params (dict): Optional query parameters
            method (str): HTTP method (GET, POST, etc.)
            data (dict): Optional request body data
            
        Returns:
            API response data
        """
        url = self.base_url + endpoint
        
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=self.headers, params=params or {})
            elif method.upper() == 'POST':
                response = requests.post(url, headers=self.headers, params=params or {}, json=data)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
                
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in API call: {e}")
            raise
    
    def execute_query(self, query_id: int, parameters: Optional[Dict] = None) -> str:
        """
        Execute a Dune query and return execution ID.
        
        Args:
            query_id (int): Dune query ID
            parameters (dict): Optional query parameters
            
        Returns:
            str: Execution ID
        """
        endpoint = f'/query/{query_id}/execute'
        data = {}
        if parameters:
            data['query_parameters'] = parameters
            
        response = self.call_dune_api(endpoint, method='POST', data=data)
        return response.get('execution_id')
    
    def get_execution_status(self, execution_id: str) -> Dict:
        """
        Get execution status.
        
        Args:
            execution_id (str): Execution ID
            
        Returns:
            dict: Execution status information
        """
        endpoint = f'/execution/{execution_id}/status'
        return self.call_dune_api(endpoint)
    
    def get_execution_results(self, execution_id: str) -> Dict:
        """
        Get execution results.
        
        Args:
            execution_id (str): Execution ID
            
        Returns:
            dict: Execution results
        """
        endpoint = f'/execution/{execution_id}/results'
        return self.call_dune_api(endpoint) 