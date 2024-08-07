select
    *

from {{ source('plaid', 'institutions') }}

qualify row_number() over(partition by institution_id order by loaded_at desc) = 1