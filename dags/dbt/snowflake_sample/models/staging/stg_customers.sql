select
    c_acctbal AS account_balance,
    c_address AS address,
    c_comment AS comment,
    c_custkey AS customer_key,
    c_mktsegment AS market_segment,
    c_name AS name,
    c_nationkey AS nation_key,
    c_phone AS phone
from 
    {{ source('tpch', 'customer') }}