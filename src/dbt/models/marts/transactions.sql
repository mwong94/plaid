select
    t.account_id
    , a.bank_name
    , a.name as account_name
    , t.account_owner
    , try_to_decimal(t.amount::text, 9, 2) as amount
    , t.authorized_date
    , t.authorized_datetime
    , t.category_0
    , t.category_1
    , t.category_2
    , t.categories
    , t.category_id
    , t.check_number
    , t.counterparties
    , t.transaction_date
    , t.datetime
    , t.iso_currency_code
    , t.location
    , t.location__address
    , t.location__city
    , t.location__country
    , t.location__lat
    , t.location__lon
    , t.location__postal_code
    , t.location__region
    , t.location__store_number
    , t.logo_url
    , t.merchant_entity_id
    , t.merchant_name
    , t.name
    , initcap(coalesce(t.merchant_name, t.name)) as description
    , t.payment_channel
    , t.payment_meta__by_order_of
    , t.payment_meta__payee
    , t.payment_meta__payer
    , t.payment_meta__payment_method
    , t.payment_meta__payment_processor
    , t.payment_meta__ppd_id
    , t.payment_meta__reason
    , t.payment_meta__reference_number
    , t.pending
    , t.pending_transaction_id
    , t.personal_finance_category__confidence_level
    , t.personal_finance_category__detailed
    , t.personal_finance_category__primary
    , t.personal_finance_category_icon_url
    , t.transaction_code
    , t.transaction_id
    , t.transaction_type
    , t.unofficial_currency_code
    , t.website

from {{ ref('stg_transactions') }} as t

left outer join {{ ref('accounts') }} as a
on t.account_id = a.account_id

where not t.merchant_entity_id in (select distinct merchant_id from {{ source('plaid', 'merchant_filter') }})
and not lower(t.merchant_name) in (select distinct merchant_name from {{ source('plaid', 'merchant_filter') }})
and not lower(t.name) in (select distinct name from {{ source('plaid', 'merchant_filter') }})

qualify row_number() over(partition by t.transaction_id order by t.loaded_at desc) = 1