{% macro deduplicate_data(column_names, order_by, relation) %}

{% set dedup_query %}
SELECT
      * EXCEPT(row_number)
    FROM (
      SELECT
        *,
        ROW_NUMBER()
        OVER (PARTITION BY {{ column_names }} order by {{ order_by }} desc ) row_number
      FROM
        {{ relation }}
    )
    WHERE
      row_number = 1
{% endset %}

{{ return(dedup_query) }}

{% endmacro %}

{% macro get_dedup_methods() %}

{{ return(deduplicate_data('payment_method, order_id', 'id', ref('raw_payments'))) }}

{% endmacro %}
