select
    ID AS payment_id,
    "orderID" AS order_id,
    "paymentMethod" AS payment_method,
    amount AS amount_australian_dollar,
    created AS processed_date
from raw.stripe.payment