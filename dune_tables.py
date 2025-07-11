from typing import List, Optional, Dict, Any
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser.ast import Constant
import pandas as pd
import time


class BalancesTable(APITable):
    """Table for EVM token balances across multiple chains."""
    
    def get_columns(self) -> List[str]:
        return [
            'chain', 'chain_id', 'address', 'amount', 'symbol', 'name', 'decimals', 
            'price_usd', 'value_usd', 'pool_size', 'low_liquidity', 'wallet_address'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get token balances for an address."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions - require wallet_address
        wallet_address = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'wallet_address' and op == '=':
                wallet_address = arg2
        
        if not wallet_address:
            return pd.DataFrame(columns=self.get_columns())
        
        # Get balances from Sim API
        endpoint = f'/evm/balances/{wallet_address}'
        response = self.handler.call_dune_api(endpoint)
        
        if response and 'balances' in response:
            rows = []
            for balance in response['balances']:
                rows.append([
                    balance.get('chain'),
                    balance.get('chain_id'),
                    balance.get('address'),
                    balance.get('amount'),
                    balance.get('symbol'),
                    balance.get('name'),
                    balance.get('decimals'),
                    balance.get('price_usd'),
                    balance.get('value_usd'),
                    balance.get('pool_size'),
                    balance.get('low_liquidity'),
                    response.get('wallet_address')
                ])
            return pd.DataFrame(rows, columns=self.get_columns())
        
        return pd.DataFrame(columns=self.get_columns())


class TransactionsTable(APITable):
    """Table for EVM transactions."""
    
    def get_columns(self) -> List[str]:
        return [
            'chain', 'chain_id', 'address', 'block_time', 'block_number', 'index', 'hash', 
            'block_hash', 'value', 'transaction_type', 'from', 'to', 'nonce', 'gas_price', 
            'gas_used', 'effective_gas_price', 'success', 'data'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get transactions for an address."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions - require wallet_address
        wallet_address = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'wallet_address' and op == '=':
                wallet_address = arg2
        
        if not wallet_address:
            return pd.DataFrame(columns=self.get_columns())
        
        # Get transactions from Sim API
        endpoint = f'/evm/transactions/{wallet_address}'
        response = self.handler.call_dune_api(endpoint)
        
        if response and 'transactions' in response:
            rows = []
            for tx in response['transactions']:
                rows.append([
                    tx.get('chain'),
                    tx.get('chain_id'),
                    tx.get('address'),
                    tx.get('block_time'),
                    tx.get('block_number'),
                    tx.get('index'),
                    tx.get('hash'),
                    tx.get('block_hash'),
                    tx.get('value'),
                    tx.get('transaction_type'),
                    tx.get('from'),
                    tx.get('to'),
                    tx.get('nonce'),
                    tx.get('gas_price'),
                    tx.get('gas_used'),
                    tx.get('effective_gas_price'),
                    tx.get('success'),
                    tx.get('data')
                ])
            return pd.DataFrame(rows, columns=self.get_columns())
        
        return pd.DataFrame(columns=self.get_columns())


class CollectiblesTable(APITable):
    """Table for EVM collectibles/NFTs."""
    
    def get_columns(self) -> List[str]:
        return [
            'contract_address', 'token_standard', 'token_id', 'chain', 'chain_id', 'name', 
            'symbol', 'metadata', 'balance', 'last_acquired', 'wallet_address'
        ]
    
    def select(self, query) -> pd.DataFrame:
        """Get collectibles for an address."""
        conditions = extract_comparison_conditions(query.where)
        
        # Parse conditions - require wallet_address
        wallet_address = None
        
        for op, arg1, arg2 in conditions:
            if arg1 == 'wallet_address' and op == '=':
                wallet_address = arg2
        
        if not wallet_address:
            return pd.DataFrame(columns=self.get_columns())
        
        # Get collectibles from Sim API
        endpoint = f'/evm/collectibles/{wallet_address}'
        response = self.handler.call_dune_api(endpoint)
        
        if response and 'entries' in response:
            rows = []
            for nft in response['entries']:
                rows.append([
                    nft.get('contract_address'),
                    nft.get('token_standard'),
                    nft.get('token_id'),
                    nft.get('chain'),
                    nft.get('chain_id'),
                    nft.get('name'),
                    nft.get('symbol'),
                    str(nft.get('metadata')) if nft.get('metadata') else None,
                    nft.get('balance'),
                    nft.get('last_acquired'),
                    response.get('address')
                ])
            return pd.DataFrame(rows, columns=self.get_columns())
        
        return pd.DataFrame(columns=self.get_columns())


# Legacy tables for backward compatibility (return empty results)
class QueriesTable(APITable):
    """Legacy table - not supported in Sim API."""
    
    def get_columns(self) -> List[str]:
        return ['query_id', 'name', 'description']
    
    def select(self, query) -> pd.DataFrame:
        return pd.DataFrame(columns=self.get_columns())


class ExecutionsTable(APITable):
    """Legacy table - not supported in Sim API."""
    
    def get_columns(self) -> List[str]:
        return ['execution_id', 'query_id', 'state']
    
    def select(self, query) -> pd.DataFrame:
        return pd.DataFrame(columns=self.get_columns())


class ResultsTable(APITable):
    """Legacy table - not supported in Sim API."""
    
    def get_columns(self) -> List[str]:
        return ['execution_id', 'query_id', 'row_number']
    
    def select(self, query) -> pd.DataFrame:
        return pd.DataFrame(columns=self.get_columns())


class ContractsTable(APITable):
    """Legacy table - not supported in Sim API."""
    
    def get_columns(self) -> List[str]:
        return ['address', 'blockchain', 'name']
    
    def select(self, query) -> pd.DataFrame:
        return pd.DataFrame(columns=self.get_columns())


class DEXTable(APITable):
    """Legacy table - not supported in Sim API."""
    
    def get_columns(self) -> List[str]:
        return ['pair_address', 'blockchain', 'dex_name']
    
    def select(self, query) -> pd.DataFrame:
        return pd.DataFrame(columns=self.get_columns())


class MarketsTable(APITable):
    """Legacy table - not supported in Sim API."""
    
    def get_columns(self) -> List[str]:
        return ['market_type', 'blockchain', 'project_name']
    
    def select(self, query) -> pd.DataFrame:
        return pd.DataFrame(columns=self.get_columns()) 