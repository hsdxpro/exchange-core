//! Independent reference models built from `BTreeMap` and plain FIFO vectors.
//!
//! These deliberately share no code with the engine: the engine is a bitmap
//! ladder over a fixed slot arena, the models are ordered maps of queues. Any
//! divergence between the two is a real behavioral difference.

use crate::{ExecutionReport, Fill, OrderError, OrderView, Side, TimeInForce, mix64};
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RefOrder {
    id: u32,
    quantity: u32,
}

#[derive(Clone, Copy, Debug)]
struct Location {
    side: Side,
    price: u32,
}

#[derive(Clone, Copy, Debug, Default)]
struct LiquidityPreview {
    executable_quantity: u32,
    releases_slot: bool,
}

#[derive(Clone, Debug)]
pub struct ReferenceL3 {
    max_order_ids: usize,
    levels: [BTreeMap<u32, Vec<RefOrder>>; 2],
    locations: HashMap<u32, Location>,
}

impl ReferenceL3 {
    /// `max_orders` is accepted for call-site symmetry with the engine and
    /// deliberately unused: the engine's pool grows on demand, so capacity is
    /// not a behaviour either side can differ on below the `u32` index space.
    #[must_use]
    pub fn new(max_orders: usize, max_order_ids: usize) -> Self {
        let _ = max_orders;
        Self {
            max_order_ids,
            levels: [BTreeMap::new(), BTreeMap::new()],
            locations: HashMap::new(),
        }
    }

    #[must_use]
    pub fn live_orders(&self) -> u32 {
        self.locations.len() as u32
    }

    #[must_use]
    pub fn order(&self, id: u32) -> Option<OrderView> {
        let location = self.locations.get(&id)?;
        let queue = self.levels[location.side.index()].get(&location.price)?;
        let order = queue.iter().find(|order| order.id == id)?;
        Some(OrderView {
            id,
            side: location.side,
            price: location.price,
            quantity: order.quantity,
        })
    }

    #[must_use]
    pub fn best_bid(&self) -> Option<u32> {
        self.levels[0].keys().next_back().copied()
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<u32> {
        self.levels[1].keys().next().copied()
    }

    #[must_use]
    pub fn would_cross(&self, side: Side, price: u32) -> bool {
        match side {
            Side::Bid => self.best_ask().is_some_and(|best| best <= price),
            Side::Ask => self.best_bid().is_some_and(|best| best >= price),
        }
    }

    #[must_use]
    pub fn level_quantity(&self, side: Side, price: u32) -> u64 {
        self.levels[side.index()]
            .get(&price)
            .map(|queue| queue.iter().map(|order| u64::from(order.quantity)).sum())
            .unwrap_or(0)
    }

    #[must_use]
    pub fn orders_at_level(&self, side: Side, price: u32) -> Vec<OrderView> {
        self.levels[side.index()]
            .get(&price)
            .map(|queue| {
                queue
                    .iter()
                    .map(|order| OrderView {
                        id: order.id,
                        side,
                        price,
                        quantity: order.quantity,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn add_passive(
        &mut self,
        id: u32,
        side: Side,
        price: u32,
        quantity: u32,
    ) -> Result<(), OrderError> {
        if price >= crate::PRICE_LIMIT {
            return Err(OrderError::PriceOutOfDomain);
        }
        self.validate_new_order(id, quantity)?;
        if self.would_cross(side, price) {
            return Err(OrderError::WouldCross);
        }
        self.append(id, side, price, quantity);
        Ok(())
    }

    pub fn cancel(&mut self, id: u32) -> Result<(), OrderError> {
        if id as usize >= self.max_order_ids {
            return Err(OrderError::OrderIdOutOfRange);
        }
        if !self.locations.contains_key(&id) {
            return Err(OrderError::UnknownOrderId);
        }
        self.erase(id);
        Ok(())
    }

    pub fn amend_down(&mut self, id: u32, new_quantity: u32) -> Result<(), OrderError> {
        if id as usize >= self.max_order_ids {
            return Err(OrderError::OrderIdOutOfRange);
        }
        let Some(&location) = self.locations.get(&id) else {
            return Err(OrderError::UnknownOrderId);
        };
        let queue = self.levels[location.side.index()]
            .get_mut(&location.price)
            .expect("live order must have a level");
        let order = queue
            .iter_mut()
            .find(|order| order.id == id)
            .expect("live order must be in its level queue");
        if new_quantity > order.quantity {
            return Err(OrderError::QuantityIncreaseNotAllowed);
        }
        if new_quantity == 0 {
            self.erase(id);
            return Ok(());
        }
        order.quantity = new_quantity;
        Ok(())
    }

    pub fn cancel_replace_with<F>(
        &mut self,
        existing_id: u32,
        replacement_id: u32,
        new_price: u32,
        new_quantity: u32,
        on_fill: F,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        if new_price >= crate::PRICE_LIMIT {
            return Err(OrderError::PriceOutOfDomain);
        }
        if existing_id as usize >= self.max_order_ids {
            return Err(OrderError::OrderIdOutOfRange);
        }
        let Some(&location) = self.locations.get(&existing_id) else {
            return Err(OrderError::UnknownOrderId);
        };
        if replacement_id == existing_id {
            return Err(OrderError::ReplacementIdMustDiffer);
        }
        self.validate_new_order(replacement_id, new_quantity)?;

        self.erase(existing_id);
        self.submit_limit_impl(
            replacement_id,
            location.side,
            new_price,
            new_quantity,
            TimeInForce::GoodTillCancel,
            on_fill,
            true,
        )
    }

    pub fn submit_limit_with<F>(
        &mut self,
        id: u32,
        side: Side,
        limit_price: u32,
        quantity: u32,
        time_in_force: TimeInForce,
        on_fill: F,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        if limit_price >= crate::PRICE_LIMIT {
            return Err(OrderError::PriceOutOfDomain);
        }
        self.submit_limit_impl(
            id,
            side,
            limit_price,
            quantity,
            time_in_force,
            on_fill,
            false,
        )
    }

    #[must_use]
    pub fn state_hash(&self) -> u64 {
        let mut hash = mix64(self.locations.len() as u64 + crate::STATE_HASH_SEED);
        for (side, levels) in self.levels.iter().enumerate() {
            for (&price, queue) in levels {
                for (rank, order) in queue.iter().enumerate() {
                    hash = mix64(
                        hash ^ ((side as u64) << 63)
                            ^ (u64::from(price) << 40)
                            ^ (u64::from(order.id) << 8)
                            ^ u64::from(order.quantity)
                            ^ (rank as u64 + 1),
                    );
                }
            }
        }
        hash
    }

    fn validate_new_order(&self, id: u32, quantity: u32) -> Result<(), OrderError> {
        if quantity == 0 {
            return Err(OrderError::QuantityZero);
        }
        if id as usize >= self.max_order_ids {
            return Err(OrderError::OrderIdOutOfRange);
        }
        if self.locations.contains_key(&id) {
            return Err(OrderError::DuplicateOrderId);
        }
        Ok(())
    }

    fn preview_liquidity(
        &self,
        taker_side: Side,
        limit_price: u32,
        requested_quantity: u32,
    ) -> LiquidityPreview {
        let mut preview = LiquidityPreview::default();
        let mut remaining = requested_quantity;
        let maker_levels = &self.levels[taker_side.opposite().index()];
        let ordered: Box<dyn Iterator<Item = (&u32, &Vec<RefOrder>)>> = match taker_side {
            Side::Bid => Box::new(maker_levels.iter()),
            Side::Ask => Box::new(maker_levels.iter().rev()),
        };

        for (&maker_price, queue) in ordered {
            if remaining == 0 || !crosses(taker_side, limit_price, maker_price) {
                break;
            }
            for maker in queue {
                if remaining == 0 {
                    break;
                }
                let fill = remaining.min(maker.quantity);
                remaining -= fill;
                preview.executable_quantity += fill;
                preview.releases_slot |= fill == maker.quantity;
            }
        }
        preview
    }

    // Mirrors the engine entry point one-for-one, including the flag that lets
    // `cancel_replace` skip re-validating an already-validated replacement.
    #[allow(clippy::too_many_arguments)]
    fn submit_limit_impl<F>(
        &mut self,
        id: u32,
        side: Side,
        limit_price: u32,
        quantity: u32,
        time_in_force: TimeInForce,
        mut on_fill: F,
        validation_already_performed: bool,
    ) -> Result<ExecutionReport, OrderError>
    where
        F: FnMut(Fill),
    {
        if !validation_already_performed {
            self.validate_new_order(id, quantity)?;
        }

        if time_in_force == TimeInForce::PostOnly {
            if self.would_cross(side, limit_price) {
                return Err(OrderError::WouldCross);
            }
            self.append(id, side, limit_price, quantity);
            return Ok(ExecutionReport {
                rested_quantity: quantity,
                ..ExecutionReport::default()
            });
        }

        // The engine's pool grows on demand, so capacity is not a behaviour
        // to model below the u32 index space; only FOK still previews.
        let preview = if time_in_force == TimeInForce::FillOrKill {
            self.preview_liquidity(side, limit_price, quantity)
        } else {
            LiquidityPreview::default()
        };

        if time_in_force == TimeInForce::FillOrKill && preview.executable_quantity < quantity {
            return Err(OrderError::InsufficientLiquidity);
        }

        let mut report = ExecutionReport::default();
        let mut remaining = quantity;
        let maker_side = side.opposite();

        while remaining != 0 {
            let best = match side {
                Side::Bid => self.best_ask(),
                Side::Ask => self.best_bid(),
            };
            let Some(maker_price) = best else {
                break;
            };
            if !crosses(side, limit_price, maker_price) {
                break;
            }

            let queue = self.levels[maker_side.index()]
                .get_mut(&maker_price)
                .expect("best price must have a queue");
            let maker = queue[0];
            let fill_quantity = remaining.min(maker.quantity);
            let fill = Fill {
                maker_order_id: maker.id,
                maker_side,
                price: maker_price,
                quantity: fill_quantity,
            };

            remaining -= fill_quantity;
            report.fills += 1;
            report.traded_quantity += u64::from(fill_quantity);
            report.notional_ticks += u64::from(maker_price) * u64::from(fill_quantity);
            report.report_hash = mix64(
                report.report_hash
                    ^ u64::from(maker.id)
                    ^ (u64::from(maker_price) << 32)
                    ^ (u64::from(fill_quantity) << 1)
                    ^ maker_side.index() as u64,
            );

            if fill_quantity == maker.quantity {
                self.erase(maker.id);
            } else {
                queue[0].quantity -= fill_quantity;
            }
            on_fill(fill);
        }

        if remaining != 0 {
            if time_in_force == TimeInForce::GoodTillCancel {
                self.append(id, side, limit_price, remaining);
                report.rested_quantity = remaining;
            } else {
                report.canceled_quantity = remaining;
            }
        }
        Ok(report)
    }

    fn append(&mut self, id: u32, side: Side, price: u32, quantity: u32) {
        self.levels[side.index()]
            .entry(price)
            .or_default()
            .push(RefOrder { id, quantity });
        self.locations.insert(id, Location { side, price });
    }

    fn erase(&mut self, id: u32) {
        let Some(location) = self.locations.remove(&id) else {
            return;
        };
        let levels = &mut self.levels[location.side.index()];
        let Some(queue) = levels.get_mut(&location.price) else {
            return;
        };
        if let Some(index) = queue.iter().position(|order| order.id == id) {
            queue.remove(index);
        }
        if queue.is_empty() {
            levels.remove(&location.price);
        }
    }
}

fn crosses(taker_side: Side, limit_price: u32, maker_price: u32) -> bool {
    match taker_side {
        Side::Bid => maker_price <= limit_price,
        Side::Ask => maker_price >= limit_price,
    }
}
