//! The coordinator describes the strategy. The accepting exchange owns its progress.
use super::{
    ClientOrderId, Error, OrderKey, Price, Quantity, Shares, Stamp,
    chaos_exchange::ChaosExchange,
    order_fsm::{Delivery, OrderMode, OrderRequest, OrderStatus},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Fallback {
    id: ClientOrderId,
    limit: Price,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
    Resting { order: OrderKey, filled: Shares },
    AwaitingReconciliation(OrderKey),
    Complete { filled: Shares },
}
#[derive(Clone, Copy, Debug)]
pub struct Step {
    pub escalate: bool,
    pub submit_receipt: Delivery,
    pub cancel_receipt: Delivery,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct HybridPlan {
    pub(super) maker: OrderRequest,
    fallback: Fallback,
}
impl HybridPlan {
    pub(super) fn fallback_id(&self) -> ClientOrderId {
        self.fallback.id
    }

    pub(super) fn taker(&self, quantity: Quantity) -> OrderRequest {
        OrderRequest::new(
            self.fallback.id,
            self.maker.account(),
            self.maker.asset().clone(),
            self.maker.side(),
            self.fallback.limit,
            quantity,
            OrderMode::ImmediateOrCancel,
        )
    }
}

enum Strategy {
    Taker(OrderRequest),
    Hybrid(HybridPlan),
}
pub struct HybridCoordinator(Strategy);
impl HybridCoordinator {
    pub fn taker(request: OrderRequest) -> Result<Self, Error> {
        if request.mode() != OrderMode::ImmediateOrCancel {
            return Err(Error::InvalidValue);
        }
        Ok(Self(Strategy::Taker(request)))
    }

    pub fn hybrid(
        maker: OrderRequest,
        taker_id: ClientOrderId,
        taker_limit: Price,
    ) -> Result<Self, Error> {
        if maker.mode() != OrderMode::PostOnly || maker.id() == taker_id {
            return Err(Error::InvalidValue);
        }
        Ok(Self(Strategy::Hybrid(HybridPlan {
            maker,
            fallback: Fallback {
                id: taker_id,
                limit: taker_limit,
            },
        })))
    }

    pub fn step(
        &self,
        exchange: &mut ChaosExchange,
        signal: Stamp,
        step: Step,
    ) -> Result<Progress, Error> {
        self.step_at(exchange, Some(signal), step)
    }
    pub(crate) fn step_at(
        &self,
        exchange: &mut ChaosExchange,
        signal: Option<Stamp>,
        step: Step,
    ) -> Result<Progress, Error> {
        match &self.0 {
            Strategy::Taker(request) => {
                let receipt = exchange.submit_at(request.clone(), signal, step.submit_receipt)?;
                Ok(progress(receipt.key(), receipt.status(), Shares::ZERO))
            }
            Strategy::Hybrid(plan) => exchange.step_hybrid(plan, signal, step),
        }
    }
}
pub(super) fn progress(key: OrderKey, status: OrderStatus, previous: Shares) -> Progress {
    match status {
        OrderStatus::Working { filled, .. } => Progress::Resting { order: key, filled },
        OrderStatus::Unconfirmed => Progress::AwaitingReconciliation(key),
        OrderStatus::Closed { filled, .. } => Progress::Complete {
            filled: Shares::from_micros(previous.micros() + filled.micros()),
        },
    }
}
