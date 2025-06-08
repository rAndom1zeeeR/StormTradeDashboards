with storm_update_position as (
  select * from stormtrade_ton.update_position
), storm_complete_order as (
  select * from stormtrade_ton.complete_order
), storm_execute_order as (
  select * from stormtrade_ton.execute_order
), storm_trade_notification as (
  select * from stormtrade_ton.trade_notification
),
position_sizes_raw as (
    select user_position, sup.direction, position_size, tx_lt as created_lt from storm_update_position sup
    union
    select user_position as position_addr, sco.direction, position_size, tx_lt as created_lt from storm_complete_order sco
), position_sizes as (
  select  distinct * from position_sizes_raw
)
, prev_position as (
 select user_position, direction, created_lt, coalesce(lag(position_size, 1) over(partition by  user_position, direction order by created_lt asc), 0) as prev_position
 from position_sizes
),
positions_raw as (
  select distinct stn.trader_addr, sup.user_position, asset_id from storm_update_position sup
  join ton.messages notif_message_out on notif_message_out.block_date = sup.block_date and notif_message_out.tx_hash = sup.tx_hash and notif_message_out.direction = 'out' -- vAMM -> vault
  join ton.messages notif_message_in on notif_message_out.block_date = notif_message_in.block_date
    and notif_message_out.msg_hash = notif_message_in.msg_hash and notif_message_out.direction = 'in' -- incoming messages for vault
  join storm_trade_notification stn on stn.tx_hash = notif_message_out.tx_hash
  where origin_op != 3427973859
  union
  select distinct seo.trader_addr , user_position, oracle_asset_id as asset_id  from storm_execute_order seo
), positions as (
  select distinct trader_addr, user_position, max(asset_id) as asset_id from positions_raw
  group by 1, 2
),
position_open as (
  select tx_hash,  vault_token, 'open' as event_type, positions.trader_addr, positions.asset_id,
  tx_lt as created_lt, tx_now, sup.direction, position_size,
   1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve as oracle_price, coalesce (abs(prev_position - position_size ) * 1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve  / 1e18, position_open_notional / 1e9)  as volume, position_fee, sup.user_position from storm_update_position sup
  --1.0 * oracle_price / 1e9 oracle_price, coalesce (abs(prev_position - position_size ) * 1.0 * oracle_price / 1e18, position_open_notional / 1e9)  as volume, position_fee, sup.user_position from storm_update_position sup
  join positions on positions.user_position = sup.user_position
  join prev_position pp on pp.user_position = sup.user_position and pp.direction = sup.direction and pp.created_lt = sup.tx_lt
  where origin_op = 2774268195
), position_close as (
  select tx_hash, vault_token, case when origin_op = 1556101853 then 'close' else 'liquidation' end as event_type, positions.trader_addr, positions.asset_id,
  tx_lt as created_lt, tx_now, sup.direction, position_size,
  1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve as oracle_price, abs(prev_position - position_size ) * 1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve  / 1e18 as volume, position_fee ,sup.user_position from storm_update_position sup
  -- 1.0 * oracle_price /1e9 as oracle_price, abs(prev_position - position_size ) * 1.0 * oracle_price  / 1e18 as volume, position_fee ,sup.user_position from storm_update_position sup
  join positions on positions.user_position = sup.user_position
  join prev_position pp on pp.user_position = sup.user_position and pp.direction = sup.direction and pp.created_lt = sup.tx_lt
  where (origin_op = 1556101853 or origin_op = 3427973859)
), tp_sl as (
  select tx_hash, vault_token, order_type as event_type, positions.trader_addr, positions.asset_id,
  tx_lt as created_lt, tx_now, sco.direction, position_size,
  1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve as oracle_price, abs(prev_position - position_size ) * 1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve  / 1e18 as volume, position_fee , sco.user_position from storm_complete_order sco
  --1.0 * oracle_price  /1e9 as oracle_price, abs(prev_position - position_size ) * 1.0 * oracle_price / 1e18 as volume, position_fee , sco.user_position from storm_complete_order sco
  join positions on positions.user_position = sco.user_position
  join prev_position pp on pp.user_position = sco.user_position and pp.direction = sco.direction and pp.created_lt = sco.tx_lt
  where origin_op = 1556101853
), open_limit as (
  select  tx_hash, vault_token, 'open' as event_type, positions.trader_addr, positions.asset_id,
  tx_lt as created_lt, tx_now, sco.direction, position_size,
  1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve as oracle_price, coalesce(abs(prev_position - position_size ) * 1.0 * quote_asset_weight * quote_asset_reserve / base_asset_reserve  / 1e18, position_open_notional / 1e9)  as volume, position_fee , sco.user_position from storm_complete_order sco
  --1.0 * oracle_price /1e9 as oracle_price, coalesce(abs(prev_position - position_size ) * 1.0 * oracle_price  / 1e18, position_open_notional / 1e9)  as volume, position_fee , sco.user_position from storm_complete_order sco
  join positions on positions.user_position = sco.user_position
  join prev_position pp on pp.user_position = sco.user_position and pp.direction = sco.direction and pp.created_lt = sco.tx_lt
  where origin_op = 2774268195
),
storm_events as (
    select * from position_open
    union all
    select * from position_close
    union all
    select * from tp_sl
    union all
    select * from open_limit
)

select date_trunc('day', from_unixtime(tx_now)) as day, sum(volume) as total_volume from storm_events
group by 1
order by 1 desc