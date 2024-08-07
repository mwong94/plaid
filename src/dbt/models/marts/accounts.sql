select
    account_id
    , i.name as bank_name
    , balance_available
    , balance_current
    , balance_limit
    , balance_iso_currency_code
    , balance_unofficial_currency_code
    , mask
    , name
    , official_name
    , persistent_account_id
    , type
    , subtype
    , loaded_at

from {{ source('plaid', 'accounts') }} as a

left outer join {{ ref('stg_institutions') }} as i
on a.institution_id = i.institution_id

qualify row_number() over(partition by account_id order by loaded_at desc) = 1