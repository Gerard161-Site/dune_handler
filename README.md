# Dune Analytics Handler

The Dune Analytics handler for MindsDB provides seamless integration with the Dune Analytics API, enabling you to access blockchain analytics data through SQL queries and execute custom Dune queries directly from your MindsDB instance.

## Implementation

This handler is implemented using the Dune Analytics API and provides access to blockchain analytics data through SQL queries.

## Dune Analytics API

Dune Analytics is a powerful platform that allows users to query blockchain data using SQL. It provides access to decoded blockchain data across multiple networks including Ethereum, Polygon, Binance Smart Chain, and more. The platform offers both community queries and professional analytics capabilities.

## Connection

### Parameters

* `api_key`: Your Dune Analytics API key (required)
* `base_url`: Dune Analytics API base URL (default: `https://api.dune.com/api/v1`)

### Example Connection

```sql
CREATE DATABASE dune_datasource
WITH ENGINE = "dune",
PARAMETERS = {
    "api_key": "your_dune_api_key_here",
    "base_url": "https://api.dune.com/api/v1"  
};
```

## Getting an API Key

1. Visit [Dune Analytics](https://dune.com/)
2. Sign up for an account
3. Navigate to your account settings
4. Generate an API key in the API section
5. Choose your subscription plan (free tier available with limitations)

## Usage

The available tables are:

* `queries` - Query management and metadata
* `executions` - Query execution tracking
* `results` - Query results data
* `contracts` - Trending smart contracts
* `dex` - DEX (Decentralized Exchange) data
* `markets` - Market statistics and analytics

### Queries Table

Manage and access query metadata:

```sql
-- Get query information
SELECT * FROM dune_datasource.queries 
WHERE query_id = 123456;
```

### Executions Table

Track and manage query executions:

```sql
-- Execute a query and get execution info
SELECT * FROM dune_datasource.executions 
WHERE query_id = 123456;

-- Check execution status
SELECT * FROM dune_datasource.executions 
WHERE execution_id = 'your_execution_id_here';
```

### Results Table

Access query results:

```sql
-- Get results from a query execution
SELECT * FROM dune_datasource.results 
WHERE execution_id = 'your_execution_id_here'
LIMIT 100;

-- Execute query and get results
SELECT * FROM dune_datasource.results 
WHERE query_id = 123456
LIMIT 50;
```

### Contracts Table

Get trending smart contracts data:

```sql
-- Get trending contracts
SELECT * FROM dune_datasource.contracts 
ORDER BY volume_usd DESC 
LIMIT 20;

-- Filter by blockchain
SELECT * FROM dune_datasource.contracts 
WHERE blockchain = 'ethereum'
ORDER BY users_count DESC;

-- Filter by category
SELECT * FROM dune_datasource.contracts 
WHERE category = 'defi'
ORDER BY txns_count DESC;
```

### DEX Table

Access DEX trading data:

```sql
-- Get top DEX pairs by volume
SELECT * FROM dune_datasource.dex
ORDER BY volume_24h DESC 
LIMIT 20;

-- Filter by blockchain
SELECT * FROM dune_datasource.dex 
WHERE blockchain = 'ethereum'
ORDER BY liquidity_usd DESC;

-- Filter by DEX
SELECT * FROM dune_datasource.dex 
WHERE dex_name = 'Uniswap V3'
ORDER BY volume_24h DESC;
```

### Markets Table

Get market analytics:

```sql
-- Get DEX market statistics
SELECT * FROM dune_datasource.markets 
WHERE market_type = 'dex'
ORDER BY volume_24h DESC;

-- Filter by blockchain
SELECT * FROM dune_datasource.markets 
WHERE blockchain = 'ethereum' 
AND market_type = 'dex';
```

## Data Types and Columns

### Queries Table
- `query_id` - Unique query identifier
- `name` - Query name
- `description` - Query description  
- `query_sql` - SQL query code
- `user_id` - Creator user ID
- `created_at` - Creation timestamp
- `updated_at` - Last update timestamp
- `is_private` - Whether query is private
- `tags` - Query tags
- `parameters` - Query parameters

### Executions Table
- `execution_id` - Unique execution identifier
- `query_id` - Associated query ID
- `state` - Execution state (pending, executing, completed, failed)
- `submitted_at` - Submission timestamp
- `expires_at` - Expiration timestamp
- `execution_started_at` - Start timestamp
- `execution_ended_at` - End timestamp
- `result_metadata` - Metadata about results

### Results Table
- `execution_id` - Execution identifier
- `query_id` - Query identifier
- `row_number` - Row number in results
- `column_name` - Column name
- `column_value` - Column value

### Contracts Table
- `address` - Contract address
- `blockchain` - Blockchain network
- `name` - Contract name
- `type` - Contract type
- `category` - Contract category
- `subcategory` - Contract subcategory
- `project_name` - Associated project
- `users_count` - Number of users
- `txns_count` - Number of transactions
- `volume_usd` - Transaction volume in USD
- `fees_usd` - Fees generated in USD
- `is_verified` - Whether contract is verified
- `created_at` - Creation timestamp

### DEX Table
- `pair_address` - Trading pair address
- `blockchain` - Blockchain network
- `dex_name` - DEX platform name
- `token0_symbol` - First token symbol
- `token1_symbol` - Second token symbol
- `token0_address` - First token address
- `token1_address` - Second token address
- `volume_24h` - 24h trading volume
- `volume_7d` - 7d trading volume
- `liquidity_usd` - Total liquidity in USD
- `fee_tier` - Fee tier
- `price_token0` - Price of token0
- `price_token1` - Price of token1
- `price_change_24h` - 24h price change
- `created_at` - Creation timestamp

### Markets Table
- `market_type` - Type of market (dex, lending, etc.)
- `blockchain` - Blockchain network
- `project_name` - Project name
- `market_share_pct` - Market share percentage
- `volume_24h` - 24h volume
- `volume_7d` - 7d volume
- `volume_30d` - 30d volume
- `users_24h` - 24h users
- `users_7d` - 7d users
- `txns_24h` - 24h transactions
- `txns_7d` - 7d transactions
- `fees_24h` - 24h fees
- `fees_7d` - 7d fees
- `rank` - Market ranking

## Advanced Use Cases

### 1. Custom Analytics
Execute custom Dune queries for specific analysis:

```sql
-- Execute a custom query and analyze results
SELECT column_name, column_value 
FROM dune_datasource.results 
WHERE query_id = 123456
AND column_name = 'daily_volume';
```

### 2. DeFi Protocol Analysis
Analyze DeFi protocols performance:

```sql
-- Top DeFi protocols by volume
SELECT project_name, volume_usd, users_count
FROM dune_datasource.contracts 
WHERE category = 'defi'
ORDER BY volume_usd DESC 
LIMIT 10;
```

### 3. DEX Trading Analysis
Monitor DEX trading patterns:

```sql
-- Uniswap V3 top pairs
SELECT token0_symbol, token1_symbol, volume_24h, liquidity_usd
FROM dune_datasource.dex 
WHERE dex_name = 'Uniswap V3'
ORDER BY volume_24h DESC 
LIMIT 20;
```

### 4. Market Trend Analysis  
Track market trends across blockchains:

```sql
-- Compare DEX volumes across chains
SELECT blockchain, SUM(volume_24h) as total_volume
FROM dune_datasource.markets 
WHERE market_type = 'dex'
GROUP BY blockchain 
ORDER BY total_volume DESC;
```

### 5. Query Execution Workflow
Manage query executions programmatically:

```sql
-- 1. Execute query
SELECT execution_id FROM dune_datasource.executions 
WHERE query_id = 123456;

-- 2. Check status  
SELECT state FROM dune_datasource.executions 
WHERE execution_id = 'your_execution_id';

-- 3. Get results when completed
SELECT * FROM dune_datasource.results 
WHERE execution_id = 'your_execution_id';
```

## Supported Blockchains

- Ethereum
- Polygon
- Binance Smart Chain (BSC)
- Arbitrum
- Optimism
- Avalanche
- Fantom
- Gnosis Chain
- And more...

## Limitations

- API requires a valid API key
- Rate limits apply based on subscription tier
- Query execution time limits
- Result size limitations
- Some advanced features require paid plans
- Query complexity limitations on free tier

## Rate Limits and Pricing

Rate limits and features vary by subscription tier:

- **Free**: Limited queries per month, basic features
- **Plus**: More queries, faster execution
- **Premium**: High query limits, priority execution
- **Enterprise**: Custom limits and features

## Query Execution Flow

1. **Submit Query**: Execute a query via `executions` table
2. **Monitor Status**: Check execution state
3. **Retrieve Results**: Access results when completed
4. **Process Data**: Use results in your analysis

## Error Handling

The handler includes comprehensive error handling for:
- Invalid API keys
- Rate limiting
- Query execution failures
- Network connectivity issues
- Invalid query parameters
- Result retrieval errors

## Best Practices

1. **Optimize Queries**: Write efficient SQL to reduce execution time
2. **Monitor Usage**: Track API usage to stay within limits
3. **Cache Results**: Store frequently accessed results locally
4. **Use Filters**: Apply WHERE clauses to reduce data transfer
5. **Batch Operations**: Group related queries together
6. **Handle Timeouts**: Account for long-running queries

## Performance Tips

- Use indexed columns in WHERE clauses
- Limit result sets with LIMIT clauses
- Use appropriate time ranges for time-series data
- Consider using Dune's materialized views for complex queries
- Monitor query execution times and optimize accordingly

## Notes

- All timestamps are Unix timestamps unless specified
- USD values are calculated using real-time price feeds
- Query results are cached for performance
- Some data may have slight delays depending on blockchain sync
- Historical data availability varies by blockchain 