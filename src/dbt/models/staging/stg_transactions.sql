select
    parse_json(jsondata) as json
    , json['account_id']::text as account_id
    , json['account_owner']::text as account_owner
    , json['amount'] as amount
    , json['authorized_date']::date as authorized_date
    , json['authorized_datetime']::timestamp_tz as authorized_datetime
    , json['category']['0']::text as category_0
    , json['category']['1']::text as category_1
    , json['category']['2']::text as category_2
    , concat_ws(',', category_0, category_1, category_2) as categories
    , json['category_id']::text as category_id
    , json['check_number']::text as check_number
    , json['counterparties'] as counterparties
    , json['date']::date as transaction_date
    , json['datetime']::timestamp_tz as datetime
    , json['iso_currency_code']::text as iso_currency_code
    , json['location'] as location
    , json['location']['address']::text as location__address
    , json['location']['city']::text as location__city
    , json['location']['country']::text as location__country
    , json['location']['lat'] as location__lat
    , json['location']['lon'] as location__lon
    , json['location']['postal_code']::text as location__postal_code
    , json['location']['region']::text as location__region
    , json['location']['store_number']::text as location__store_number
    , json['logo_url']::text as logo_url
    , json['merchant_entity_id']::text as merchant_entity_id
    , json['merchant_name']::text as merchant_name
    , json['name']::text as name
    , json['payment_channel']::text as payment_channel
    , json['payment_meta'] as payment_meta
    , json['payment_meta']['by_order_of']::text as payment_meta__by_order_of
    , json['payment_meta']['payee']::text as payment_meta__payee
    , json['payment_meta']['payer']::text as payment_meta__payer
    , json['payment_meta']['payment_method']::text as payment_meta__payment_method
    , json['payment_meta']['payment_processor']::text as payment_meta__payment_processor
    , json['payment_meta']['ppd_id']::text as payment_meta__ppd_id
    , json['payment_meta']['reason']::text as payment_meta__reason
    , json['payment_meta']['reference_number']::text as payment_meta__reference_number
    , json['pending']::boolean as pending
    , json['pending_transaction_id']::text as pending_transaction_id
    , json['personal_finance_category'] as personal_finance_category
    , json['personal_finance_category']['confidence_level']::text as personal_finance_category__confidence_level
    , json['personal_finance_category']['detailed']::text as personal_finance_category__detailed
    , json['personal_finance_category']['primary']::text as personal_finance_category__primary
    , json['personal_finance_category_icon_url']::text as personal_finance_category_icon_url
    , json['transaction_code']::text as transaction_code
    , json['transaction_id']::text as transaction_id
    , json['transaction_type']::text as transaction_type
    , json['unofficial_currency_code']::text as unofficial_currency_code
    , json['website']::text as website

from {{ source('plaid', 'transactions_json') }}