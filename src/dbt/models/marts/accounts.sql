select
    a.account_id
    , i.name as bank_name
    , a.balance_available
    , a.balance_current
    , a.balance_limit
    , a.balance_iso_currency_code
    , a.balance_unofficial_currency_code
    , a.mask
    , a.name
    , a.official_name
    , a.persistent_account_id
    , a.type
    , a.subtype
    , a.loaded_at

from {{ source('plaid', 'accounts') }} as a

left outer join {{ ref('stg_institutions') }} as i
on a.institution_id = i.institution_id

qualify row_number() over(partition by a.account_id order by a.loaded_at desc) = 1