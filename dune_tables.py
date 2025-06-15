from typing import List, Optional, Dict, Any
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser.ast import Constant
import pandas as pd
import time


class QueriesTable(APITable):
    """Table for Dune Analytics queries management."""
    
    def get_columns(self) -> List[str]:
        return [
            'query_id', 'name', 'description', 'query_sql', 'user_id', 'created_at',
            'updated_at', 'is_private', 'tags', 'parameters'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get Dune queries data."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions
        query_id = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'query_id' and op == '=':
                query_id = arg2
        
        if query_id:
            # Get specific query metadata
            try:
                response = self.handler.call_dune_api(f'/query/{query_id}')
                if response:
                    return pd.DataFrame([self._process_query_data(response)], columns=self.get_columns())
            except:
                # If query endpoint doesn't exist, return empty result
                pass
        
        return pd.DataFrame(columns=self.get_columns())
    
    def _process_query_data(self, query_data: Dict) -> List:
        """Process query metadata."""
        return [
            query_data.get('query_id'),
            query_data.get('name'),
            query_data.get('description'),
            query_data.get('query'),
            query_data.get('user_id'),
            query_data.get('created_at'),
            query_data.get('updated_at'),
            query_data.get('is_private'),
            ','.join(query_data.get('tags', [])) if query_data.get('tags') else None,
            str(query_data.get('parameters', {})) if query_data.get('parameters') else None
        ]


class ExecutionsTable(APITable):
    """Table for Dune Analytics query executions."""
    
    def get_columns(self) -> List[str]:
        return [
            'execution_id', 'query_id', 'state', 'submitted_at', 'expires_at',
            'execution_started_at', 'execution_ended_at', 'result_metadata'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get query execution data."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions
        execution_id = None
        query_id = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'execution_id' and op == '=':
                execution_id = arg2
            elif arg1 == 'query_id' and op == '=':
                query_id = arg2
        
        if execution_id:
            # Get execution status
            response = self.handler.get_execution_status(execution_id)
            if response:
                return pd.DataFrame([self._process_execution_data(response)], columns=self.get_columns())
        elif query_id:
            # Execute query and return execution info
            try:
                execution_id = self.handler.execute_query(query_id)
                if execution_id:
                    response = self.handler.get_execution_status(execution_id)
                    if response:
                        return pd.DataFrame([self._process_execution_data(response)], columns=self.get_columns())
            except:
                pass
        
        return pd.DataFrame(columns=self.get_columns())
    
    def _process_execution_data(self, execution: Dict) -> List:
        """Process execution data."""
        return [
            execution.get('execution_id'),
            execution.get('query_id'),
            execution.get('state'),
            execution.get('submitted_at'),
            execution.get('expires_at'),
            execution.get('execution_started_at'),
            execution.get('execution_ended_at'),
            str(execution.get('result_metadata', {})) if execution.get('result_metadata') else None
        ]


class ResultsTable(APITable):
    """Table for Dune Analytics query results."""
    
    def get_columns(self) -> List[str]:
        return [
            'execution_id', 'query_id', 'row_number', 'column_name', 'column_value'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get query results data."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions
        execution_id = None
        query_id = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'execution_id' and op == '=':
                execution_id = arg2
            elif arg1 == 'query_id' and op == '=':
                query_id = arg2
        
        if execution_id:
            # Get results for specific execution
            response = self.handler.get_execution_results(execution_id)
            if response and 'result' in response:
                return self._process_results_data(response['result'], execution_id)
        elif query_id:
            # Execute query and get results
            try:
                execution_id = self.handler.execute_query(query_id)
                if execution_id:
                    # Wait a moment for execution to start
                    time.sleep(2)
                    response = self.handler.get_execution_results(execution_id)
                    if response and 'result' in response:
                        return self._process_results_data(response['result'], execution_id)
            except:
                pass
        
        return pd.DataFrame(columns=self.get_columns())
    
    def _process_results_data(self, result: Dict, execution_id: str) -> pd.DataFrame:
        """Process query results into normalized format."""
        rows = []
        
        if 'rows' in result and 'metadata' in result:
            columns = [col['name'] for col in result['metadata']['column_names']]
            
            for row_idx, row_data in enumerate(result['rows']):
                for col_idx, col_name in enumerate(columns):
                    value = row_data[col_idx] if col_idx < len(row_data) else None
                    rows.append([
                        execution_id,
                        result.get('query_id'),
                        row_idx + 1,
                        col_name,
                        str(value) if value is not None else None
                    ])
        
        return pd.DataFrame(rows, columns=self.get_columns())


class ContractsTable(APITable):
    """Table for trending EVM contracts data."""
    
    def get_columns(self) -> List[str]:
        return [
            'address', 'blockchain', 'name', 'type', 'category', 'subcategory',
            'project_name', 'users_count', 'txns_count', 'volume_usd', 'fees_usd',
            'is_verified', 'created_at'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get trending contracts data."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions
        blockchain = None
        contract_type = None
        category = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'blockchain' and op == '=':
                blockchain = arg2
            elif arg1 == 'type' and op == '=':
                contract_type = arg2
            elif arg1 == 'category' and op == '=':
                category = arg2
        
        # Build endpoint with filters
        endpoint = '/contracts/trending'
        params = {}
        
        if blockchain:
            params['blockchain'] = blockchain
        if contract_type:
            params['type'] = contract_type
        if category:
            params['category'] = category
        
        # Get data from API
        response = self.handler.call_dune_api(endpoint, params)
        
        if response and isinstance(response, list):
            rows = []
            for contract in response:
                rows.append([
                    contract.get('address'),
                    contract.get('blockchain'),
                    contract.get('name'),
                    contract.get('type'),
                    contract.get('category'),
                    contract.get('subcategory'),
                    contract.get('project_name'),
                    contract.get('users_count'),
                    contract.get('txns_count'),
                    contract.get('volume_usd'),
                    contract.get('fees_usd'),
                    contract.get('is_verified'),
                    contract.get('created_at')
                ])
            return pd.DataFrame(rows, columns=self.get_columns())
        
        return pd.DataFrame(columns=self.get_columns())


class DEXTable(APITable):
    """Table for DEX (Decentralized Exchange) data."""
    
    def get_columns(self) -> List[str]:
        return [
            'pair_address', 'blockchain', 'dex_name', 'token0_symbol', 'token1_symbol',
            'token0_address', 'token1_address', 'volume_24h', 'volume_7d',
            'liquidity_usd', 'fee_tier', 'price_token0', 'price_token1',
            'price_change_24h', 'created_at'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get DEX data."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions
        blockchain = None
        dex_name = None
        token_symbol = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'blockchain' and op == '=':
                blockchain = arg2
            elif arg1 == 'dex_name' and op == '=':
                dex_name = arg2
            elif arg1 == 'token_symbol' and op == '=':
                token_symbol = arg2
        
        # Build endpoint with filters
        endpoint = '/dex/pairs'
        params = {}
        
        if blockchain:
            params['blockchain'] = blockchain
        if dex_name:
            params['dex'] = dex_name
        if token_symbol:
            params['token'] = token_symbol
        
        # Get data from API
        response = self.handler.call_dune_api(endpoint, params)
        
        if response and isinstance(response, list):
            rows = []
            for pair in response:
                rows.append([
                    pair.get('pair_address'),
                    pair.get('blockchain'),
                    pair.get('dex_name'),
                    pair.get('token0_symbol'),
                    pair.get('token1_symbol'),
                    pair.get('token0_address'),
                    pair.get('token1_address'),
                    pair.get('volume_24h'),
                    pair.get('volume_7d'),
                    pair.get('liquidity_usd'),
                    pair.get('fee_tier'),
                    pair.get('price_token0'),
                    pair.get('price_token1'),
                    pair.get('price_change_24h'),
                    pair.get('created_at')
                ])
            return pd.DataFrame(rows, columns=self.get_columns())
        
        return pd.DataFrame(columns=self.get_columns())


class MarketsTable(APITable):
    """Table for market data and statistics."""
    
    def get_columns(self) -> List[str]:
        return [
            'market_type', 'blockchain', 'project_name', 'market_share_pct',
            'volume_24h', 'volume_7d', 'volume_30d', 'users_24h', 'users_7d',
            'txns_24h', 'txns_7d', 'fees_24h', 'fees_7d', 'rank'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get market data."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions
        market_type = 'dex'  # Default to DEX markets
        blockchain = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'market_type' and op == '=':
                market_type = arg2
            elif arg1 == 'blockchain' and op == '=':
                blockchain = arg2
        
        # Build endpoint
        endpoint = f'/markets/{market_type}'
        params = {}
        
        if blockchain:
            params['blockchain'] = blockchain
        
        # Get data from API
        response = self.handler.call_dune_api(endpoint, params)
        
        if response and isinstance(response, list):
            rows = []
            for market in response:
                rows.append([
                    market_type,
                    market.get('blockchain'),
                    market.get('project_name'),
                    market.get('market_share_pct'),
                    market.get('volume_24h'),
                    market.get('volume_7d'),
                    market.get('volume_30d'),
                    market.get('users_24h'),
                    market.get('users_7d'),
                    market.get('txns_24h'),
                    market.get('txns_7d'),
                    market.get('fees_24h'),
                    market.get('fees_7d'),
                    market.get('rank')
                ])
            return pd.DataFrame(rows, columns=self.get_columns())
        
        return pd.DataFrame(columns=self.get_columns()) 