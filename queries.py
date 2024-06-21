net_exposure = """
with
selected_date as (
    select date("{date}") as `date`
),

entities_currencies as (
  SELECT 
    CASE 
      WHEN entity LIKE "%DIFC%" THEN "EPDIFC"
      WHEN entity LIKE "%Cyprus%" THEN "EPM-Cyprus"
      ELSE entity_code 
      END AS entity,
    currency
  FROM
    `ebi-dev-260310.dbt_cgabilondo_seed_treasury.entities_currencies_mapping`
),

rates as (
    select
        rate_date,
        ccy,
        rate
    from `ebury-business-intelligence.base_rate_feed.fwd_curve_reuters_days`
    where length = "SP"
    and rate_type = "mid_rate" 
    and rate_date = (select `date` from selected_date)
),
rates_to_entity_ccy as (
    select *
    from rates
    where ccy in (select distinct currency from entities_currencies)
),
rates_to_currencies as (
    select
        ccy_usd.rate_date,
        concat(ccy_usd.ccy, usd_ccy.ccy) as currency_pair,
        ccy_usd.rate / usd_ccy.rate as rate
    from rates as ccy_usd
    left join rates_to_entity_ccy as usd_ccy
      	using(rate_date)
),

brokerdeals_entities AS (
  SELECT
    * EXCEPT(cs_entity_at_tr_time),
    CASE 
      WHEN cs_entity_at_tr_time LIKE "%Cyprus%" THEN "EPM-Cyprus"
      WHEN (cs_entity_at_tr_time LIKE "%Markets%" AND cs_entity_at_tr_time NOT LIKE "%Cyprus%") THEN "EPM"
      WHEN cs_entity_at_tr_time LIKE "%Hong Kong%" THEN "EPHK"
      WHEN cs_entity_at_tr_time LIKE "%Finance%" THEN "EPF"
      WHEN cs_entity_at_tr_time LIKE "%UK%" THEN "EPUK"
      WHEN cs_entity_at_tr_time LIKE "%Canada%" THEN "EPCA"
      WHEN cs_entity_at_tr_time LIKE "%Australia%" THEN "EPAU"
      WHEN cs_entity_at_tr_time LIKE "%DIFC%" THEN "EPDIFC"
      WHEN cs_entity_at_tr_time LIKE "%Chile%" THEN "EPCHL"
      WHEN cs_entity_at_tr_time LIKE "%Switzerland%" THEN "EPCH"
      WHEN cs_entity_at_tr_time LIKE "%Belgium%" THEN "EPBE"
      WHEN cs_entity_at_tr_time LIKE "%US%" THEN "EPUS"
      WHEN cs_entity_at_tr_time LIKE "%China%" THEN "EPCN"
      WHEN cs_entity_at_tr_time LIKE "%Africa%" THEN "EPSA"
      WHEN cs_entity_at_tr_time LIKE "%Mexico%" THEN "EPME"
      WHEN cs_entity_at_tr_time LIKE "%Trans%" THEN "Trans Skills"
      WHEN cs_entity_at_tr_time = "Ebury Partners Limited" THEN "EPL"
      WHEN cs_entity_at_tr_time LIKE "%Technology%" THEN "ETL"
      ELSE cs_entity_at_tr_time
      END AS entity
  FROM
    `ebury-business-intelligence.core_dimensional.brokerdeals_gbp_mtm_historic_end_table`
),

brokerdeals as (
    select
        trades.*,
        entities.currency as entity_currency,
        case
            when
                (buy_currency = 'BRL' and buy_balance = 275000000)
                or (sell_currency = 'BRL' and sell_balance = 275000000)
                  then 'bexs_hedge'
            when lower(mapping.new_view) = "treasury earn"
                then 'earn_trades_below'
            when mapping.new_view = "BS FX Risk hedge" and trades.liquidity_source = 'Ebury'
                then 'fx_hedge_internal_below'
            when mapping.new_view = "BS FX Risk hedge"
                then 'fx_hedge_external_below'
            when mapping.new_view = 'Revenue FX Risk hedge' and trades.liquidity_source = 'Ebury'
                then 'fx_hedge_internal_above'
            when mapping.new_view = 'Revenue FX Risk hedge'
                then 'fx_hedge_external_above'
            when mapping.hedging ="FWD"
                then 'macro_hedges_above'
            when mapping.new_view = 'Treasury'
                then 'liquidity_swaps_below'
            else 'client_book_above'
      	end as book_type
    from brokerdeals_entities as trades
    left join `ebury-business-intelligence.dbt_audit.dealset_mapping` as mapping
        on lower(trades.dealset) = lower(mapping.old_view)
        or lower(trades.dealset) = lower(mapping.new_view)
    left join entities_currencies as entities
        on trades.entity = entities.entity
    where trades.eod_book = (select `date` from selected_date)
    and trades.maturity_date > trades.eod_book
    and trades.status != "Cancelled"
    and trades.buy_currency != trades.sell_currency
    and not (trades.liquidity_source = "Ebury"
             and mapping.new_view is null)
    and ((coalesce(dealset,"") != "EBPBTR3393969" AND maturity_date>="2024-04-22") OR maturity_date < "2024-04-22")
),

brokerdeals_sell_side_ccy as (
    select
        bd.*,
        bd.sell_balance * rtc.rate as sell_side_entity_ccy
    from brokerdeals as bd
    left join rates_to_currencies as rtc
        on bd.sell_currency = left(rtc.currency_pair,3)
        and bd.entity_currency = right(rtc.currency_pair, 3)
),
brokerdeals_buy_side_ccy as (
    select
        bd.*,
        bd.buy_balance * rtc.rate as buy_side_entity_ccy
    from brokerdeals_sell_side_ccy as bd
    left join rates_to_currencies as rtc
        on bd.buy_currency = left(rtc.currency_pair,3)
        and bd.entity_currency = right(rtc.currency_pair, 3)
),
sell_brokerdeals as (
  	select
        sell_currency,
        entity_currency,
        entity,
        sum(sell_balance) as sell,
        sum(gbp_sell_balance_at_spot) as sell_gbp,
        sum(sell_side_entity_ccy) as sell_entity_ccy,
        maturity_date,
        book_type
    from brokerdeals_buy_side_ccy
    group by sell_currency, entity_currency, entity, maturity_date, book_type
),
buy_brokerdeals as (
    select
        buy_currency,
        entity_currency,
        entity,
        sum(buy_balance) as buy,
        sum(gbp_buy_balance_at_spot) as buy_gbp,
        sum(buy_side_entity_ccy) as buy_entity_ccy,
        maturity_date,
        book_type
    from brokerdeals_buy_side_ccy as trades
    group by buy_currency, entity_currency,entity, maturity_date, book_type
),
sell_buy_brokerdeals as (
    select
        coalesce(sell.sell_currency, buy.buy_currency) as ccy,
        coalesce(sell.entity_currency, buy.entity_currency) as entity_ccy,
        coalesce(sell.entity, buy.entity) as entity,
        coalesce(buy.buy, 0) - coalesce(sell.sell, 0)  as net,
        coalesce(buy.buy_gbp, 0) - coalesce(sell.sell_gbp, 0) as net_gbp,
        coalesce(buy.buy_entity_ccy, 0) - coalesce(sell.sell_entity_ccy, 0) as net_entity_ccy,
        coalesce(sell.maturity_date, buy.maturity_date) as maturity_date,
        coalesce(sell.book_type, buy.book_type) as book_type,
    from sell_brokerdeals as sell
    full outer join buy_brokerdeals as buy
        on sell.sell_currency = buy.buy_currency
        and sell.entity = buy.entity
        and sell.maturity_date = buy.maturity_date
        and sell.book_type = buy.book_type
),

client_trades_entities AS (
  SELECT
    * EXCEPT(ebury_entity_at_tr_time),
    CASE 
      WHEN ebury_entity_at_tr_time LIKE "%Cyprus%" THEN "EPM-Cyprus"
      WHEN (ebury_entity_at_tr_time LIKE "%Markets%" AND ebury_entity_at_tr_time NOT LIKE "%Cyprus%") THEN "EPM"
      WHEN ebury_entity_at_tr_time LIKE "%Hong Kong%" THEN "EPHK"
      WHEN ebury_entity_at_tr_time LIKE "%Finance%" THEN "EPF"
      WHEN ebury_entity_at_tr_time LIKE "%UK%" THEN "EPUK"
      WHEN ebury_entity_at_tr_time LIKE "%Canada%" THEN "EPCA"
      WHEN ebury_entity_at_tr_time LIKE "%Australia%" THEN "EPAU"
      WHEN ebury_entity_at_tr_time LIKE "%DIFC%" THEN "EPDIFC"
      WHEN ebury_entity_at_tr_time LIKE "%Chile%" THEN "EPCHL"
      WHEN ebury_entity_at_tr_time LIKE "%Switzerland%" THEN "EPCH"
      WHEN ebury_entity_at_tr_time LIKE "%Belgium%" THEN "EPBE"
      WHEN ebury_entity_at_tr_time LIKE "%US%" THEN "EPUS"
      WHEN ebury_entity_at_tr_time LIKE "%China%" THEN "EPCN"
      WHEN ebury_entity_at_tr_time LIKE "%Africa%" THEN "EPSA"
      WHEN ebury_entity_at_tr_time LIKE "%Mexico%" THEN "EPME"
      WHEN ebury_entity_at_tr_time LIKE "%Trans%" THEN "Trans Skills"
      WHEN ebury_entity_at_tr_time = "Ebury Partners Limited" THEN "EPL"
      WHEN ebury_entity_at_tr_time LIKE "%Technology%" THEN "ETL"
      ELSE ebury_entity_at_tr_time
      END AS entity
  FROM
    `ebury-business-intelligence.core_dimensional.open_book_gbp_mtm_ifrs_historic_end_table`
),

client_trades as (
    select
        cs.* except(sell_balance, buy_balance, gbp_sell_balance_at_spot, gbp_buy_balance_at_spot),
        case
            when cs.transaction_receipt = 'EBPBTR3385437' and cs.sell_balance > 10000000
                then cs.sell_balance / 10
            else cs.sell_balance
        end as sell_balance,
        case
            when cs.transaction_receipt = 'EBPBTR3385437' and cs.buy_balance > 10000000
                then cs.buy_balance / 10
            else cs.buy_balance
        end as buy_balance,
        case
            when cs.transaction_receipt = 'EBPBTR3385437' and cs.gbp_sell_balance_at_spot > 10000000
                then cs.gbp_sell_balance_at_spot / 10
            else cs.gbp_sell_balance_at_spot
        end as gbp_sell_balance_at_spot,
        case
            when cs.transaction_receipt = 'EBPBTR3385437' and cs.gbp_buy_balance_at_spot > 10000000
                then cs.gbp_buy_balance_at_spot / 10
            else cs.gbp_buy_balance_at_spot
        end as gbp_buy_balance_at_spot,
        entities.currency as entity_currency,
        -- rem.account_name,
        -- rem.api_type
    from client_trades_entities as cs
    -- left join remove_accounts as rem
    --   	on cs.client_id = rem.account_number
    left join entities_currencies as entities
        on cs.entity = entities.entity
    where
        eod_book = (select `date` from selected_date)
        and value_date > (select `date` from selected_date)
        and buy_currency != sell_currency
        and status != "Cancelled"
  		and not coalesce(cs.internal_or_proprietary_account, false)
        -- and rem.api_type is null
),
client_trades_sell_side_ccy as (
    select
        ct.*,
        ct.sell_balance * rtc.rate as sell_side_entity_ccy
    from client_trades as ct
    left join rates_to_currencies as rtc
        on ct.sell_currency = left(rtc.currency_pair,3)
      and ct.entity_currency = right(rtc.currency_pair, 3)
),
client_trades_buy_side_ccy as (
    select
        ct.*,
        ct.buy_balance * rtc.rate as buy_side_entity_ccy
    from client_trades_sell_side_ccy as ct
    left join rates_to_currencies as rtc
        on ct.buy_currency = left(rtc.currency_pair,3)
        and ct.entity_currency = right(rtc.currency_pair, 3)
),
sell_client_trades as (
    select
        sell_currency,
        entity_currency,
        entity,
        sum(sell_balance) as sell,
        sum(gbp_sell_balance_at_spot) as sell_gbp,
        sum(sell_side_entity_ccy) as sell_entity_ccy,
        value_date
    from client_trades_buy_side_ccy
    group by sell_currency, entity_currency, entity, value_date
),
buy_client_trades as (
    select
        buy_currency,
        entity_currency,
        entity,
        sum(buy_balance) as buy,
        sum(gbp_buy_balance_at_spot) as buy_gbp,
        sum(buy_side_entity_ccy) as buy_entity_ccy,
        value_date
    from client_trades_buy_side_ccy
    group by buy_currency, entity_currency, entity, value_date
),
sell_buy_client_trades as (
    select
        coalesce(sell.sell_currency, buy.buy_currency) as ccy,
        coalesce(sell.entity_currency, buy.entity_currency) as entity_ccy,
        coalesce(sell.entity, buy.entity) as entity,
        coalesce(sell.sell, 0) - coalesce(buy.buy, 0) as net,
        coalesce(sell.sell_gbp, 0) - coalesce(buy.buy_gbp, 0) as net_gbp,
        coalesce(sell.sell_entity_ccy, 0) - coalesce(buy.buy_entity_ccy, 0) as net_entity_ccy,
        coalesce(sell.value_date, buy.value_date) as maturity_date,
      'client_book_above' as book_type
    from sell_client_trades as sell
    full outer join buy_client_trades as buy
        on sell.sell_currency = buy.buy_currency
        and sell.entity = buy.entity
        and sell.value_date = buy.value_date
),

combined_sides as (
    select
        coalesce(lp.ccy, cs.ccy) as currency,
        coalesce(lp.entity, cs.entity) as entity,
        coalesce(lp.entity_ccy, cs.entity_ccy) as entity_ccy,
        coalesce(lp.maturity_date, cs.maturity_date) as maturity_date,
        coalesce(cs.net, 0) + coalesce(lp.net, 0) as net_balance,
        coalesce(cs.net_gbp, 0) + coalesce(lp.net_gbp, 0) as net_balance_gbp,
        coalesce(cs.net_entity_ccy, 0) + coalesce(lp.net_entity_ccy, 0) as net_balance_entity_ccy,
        coalesce(lp.book_type, cs.book_type) as book_type
    from sell_buy_brokerdeals as lp
    full outer join sell_buy_client_trades as cs
        on lp.ccy = cs.ccy
        and lp.maturity_date = cs.maturity_date
        and lp.book_type = cs.book_type
        and lp.entity = cs.entity
),


above_the_line AS (
  SELECT
    currency,
    entity,
    entity_ccy,
    SUM(net_balance_gbp) AS above_exposure_gbp,
    SUM(net_balance) AS above_exposure_local_ccy,
    SUM(net_balance_entity_ccy) AS above_exposure_entity_ccy,
  FROM
    combined_sides
  WHERE
    book_type IN ("fx_hedge_external_above","fx_hedge_internal_above","macro_hedges_above","client_book_above")
  GROUP BY
    currency, entity, entity_ccy
),

below_hedges AS (
  SELECT
    currency,
    entity,
    entity_ccy,
    SUM(net_balance_gbp) AS below_exposure_gbp,
    SUM(net_balance) AS below_exposure_local_ccy,
    SUM(net_balance_entity_ccy) AS below_exposure_entity_ccy,
  FROM
    combined_sides
  WHERE
    book_type IN ("earn_trades_below","fx_hedge_external_below","fx_hedge_internal_below","liquidity_swaps_below")
  GROUP BY
    currency, entity, entity_ccy
),

below AS (
  SELECT 
    currency, 
    entity, 
    SUM(below_exposure_gbp) AS below_exposure_gbp,
    SUM(below_exposure) AS below_exposure_local_ccy,
  FROM 
    root-rarity-166622.fx_exposure.below_the_line 
  WHERE 
    date(balance_date) = date("{date}") 
  GROUP BY
    currency, entity
),

below_the_line AS (
  SELECT
    entities_currencies.currency AS entity_currency,
    below.*,
    below.below_exposure_local_ccy * rate AS below_exposure_entity_ccy,
    rate
  FROM
    below
  LEFT JOIN
    entities_currencies
  USING (entity)
  LEFT JOIN
    rates_to_currencies
  ON
    LEFT(rates_to_currencies.currency_pair,3) = below.currency
    AND RIGHT(rates_to_currencies.currency_pair,3) = entities_currencies.currency
),

all_exposure AS (
  SELECT
    COALESCE(t1.currency,t2.currency,t3.currency) AS currency,
    COALESCE(t1.entity,t2.entity,t3.entity) AS entity,
    COALESCE(t1.entity_ccy,t2.entity_currency,t3.entity_ccy) AS entity_ccy,
    COALESCE(t1.above_exposure_gbp,0) AS above_exposure_gbp,
    COALESCE(t1.above_exposure_local_ccy,0) AS above_exposure_local_ccy,
    COALESCE(t1.above_exposure_entity_ccy,0) AS above_exposure_entity_ccy,
    COALESCE(t2.below_exposure_gbp,0) + COALESCE(t3.below_exposure_gbp,0) AS below_exposure_gbp,
    COALESCE(t2.below_exposure_local_ccy,0) + COALESCE(t3.below_exposure_local_ccy,0) AS below_exposure_local_ccy,
    COALESCE(t2.below_exposure_entity_ccy,0) + COALESCE(t3.below_exposure_entity_ccy,0) AS below_exposure_entity_ccy,
    COALESCE(t1.above_exposure_gbp,0) + COALESCE(t2.below_exposure_gbp,0) + COALESCE(t3.below_exposure_gbp,0) AS net_exposure_gbp,
    COALESCE(t1.above_exposure_local_ccy,0) + COALESCE(t2.below_exposure_local_ccy,0) + COALESCE(t3.below_exposure_local_ccy,0) AS net_exposure_local_ccy,
    COALESCE(t1.above_exposure_entity_ccy,0) + COALESCE(t2.below_exposure_entity_ccy,0) + COALESCE(t3.below_exposure_entity_ccy,0) AS net_exposure_entity_ccy
  FROM
    above_the_line AS t1
  FULL OUTER JOIN
    below_the_line AS t2
  USING
    (currency,entity)
  FULL OUTER JOIN
    below_hedges AS t3
  USING
    (currency,entity)

)


SELECT 
  all_exposure.*
FROM
  all_exposure
WHERE
  entity NOT IN ("Facilitadora Pagamentos","BEXS","EPBR","FES","Newco","Trans Skills","EPSA","EMP")
"""