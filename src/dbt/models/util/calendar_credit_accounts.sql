select
    cal.date
    , a.name as account_name

from {{ source('plaid-util', 'calendar') }} as cal

join {{ ref('accounts') }} as a

where a.type = 'credit';