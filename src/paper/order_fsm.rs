//! Closed order lifecycle. Only the simulator can debit a live reservation or publish an outcome.
use super::{
    AccountId, AssetId, Cash, ClientOrderId, Price, Quantity, Shares, Side,
    fee_model::AccruedRebates,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderMode {
    PostOnly,
    ImmediateOrCancel,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderRequest {
    id: ClientOrderId,
    account: AccountId,
    asset: AssetId,
    side: Side,
    limit: Price,
    quantity: Quantity,
    mode: OrderMode,
}

impl OrderRequest {
    pub fn new(
        id: ClientOrderId,
        account: AccountId,
        asset: AssetId,
        side: Side,
        limit: Price,
        quantity: Quantity,
        mode: OrderMode,
    ) -> Self {
        Self {
            id,
            account,
            asset,
            side,
            limit,
            quantity,
            mode,
        }
    }
    pub(super) fn crosses(&self, opposite: Option<Price>) -> bool {
        opposite.is_some_and(|p| self.side.crosses(self.limit, p))
    }
    pub fn id(&self) -> ClientOrderId {
        self.id
    }
    pub fn account(&self) -> AccountId {
        self.account
    }
    pub fn asset(&self) -> &AssetId {
        &self.asset
    }
    pub fn side(&self) -> Side {
        self.side
    }
    pub fn limit(&self) -> Price {
        self.limit
    }
    pub fn quantity(&self) -> Quantity {
        self.quantity
    }
    pub fn mode(&self) -> OrderMode {
        self.mode
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseReason {
    Filled,
    Cancelled,
    UnfilledIoc,
    WouldTake,
    SelfTradePrevented,
    LostBook,
    BookCrossed,
    TickChanged,
    SessionEnded,
    DroppedBeforeExecution,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderStatus {
    Unconfirmed,
    Working { filled: Shares, remaining: Quantity },
    Closed { filled: Shares, reason: CloseReason },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    Delivered,
    /// The exchange acted but the caller lost the receipt.
    Lost,
    /// The command was lost before reaching the simulated matching engine.
    Dropped,
}

#[derive(Default)]
pub(super) struct Credit {
    pub cash: u128,
    pub shares: u64,
    pub rebates: AccruedRebates,
}
impl Credit {
    fn add(&mut self, incoming: Self) {
        self.cash += incoming.cash;
        self.shares += incoming.shares;
        self.rebates.merge(incoming.rebates);
    }
}

// Each working order owns exactly one side of collateral. No cloning or
// separate writeable reservation balance exists.
pub(super) enum Collateral {
    Buy { cash_per_micro: u128 },
    Sell,
}
impl Collateral {
    pub(super) fn held(&self, remaining: Quantity) -> Credit {
        let micros = remaining.shares().micros();
        match self {
            Self::Buy { cash_per_micro } => Credit {
                cash: *cash_per_micro * u128::from(micros),
                ..Credit::default()
            },
            Self::Sell => Credit {
                shares: micros,
                ..Credit::default()
            },
        }
    }
}

#[derive(Clone, Copy, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CancelReason {
    EdgeLost,
    Timeout,
    QuoteUnavailable,
    Disabled,
}

#[derive(Clone, Copy, serde::Serialize)]
pub(crate) struct CancelRequest {
    pub reason: CancelReason,
    pub sent_ms: u64,
    pub effective_ms: u64,
    pub input_interrupted: bool,
}

enum ExchangeState {
    Working {
        remaining: Quantity,
        collateral: Collateral,
        cancellation: Option<CancelRequest>,
    },
    Closed {
        unfilled: Shares,
        reason: CloseReason,
        at: u64,
        request: Option<CancelRequest>,
    },
}
enum Visibility {
    Confirmed,
    Unconfirmed(Credit),
}

pub(crate) struct Order {
    request: OrderRequest,
    state: ExchangeState,
    visibility: Visibility,
}

impl Order {
    pub(super) fn new(request: OrderRequest, collateral: Collateral, delivery: Delivery) -> Self {
        let visibility = match delivery {
            Delivery::Lost | Delivery::Dropped => Visibility::Unconfirmed(Credit::default()),
            Delivery::Delivered => Visibility::Confirmed,
        };
        Self {
            state: ExchangeState::Working {
                remaining: request.quantity(),
                collateral,
                cancellation: None,
            },
            request,
            visibility,
        }
    }

    pub(crate) fn cancellation(&self) -> Option<CancelRequest> {
        match &self.state {
            ExchangeState::Working { cancellation, .. } => *cancellation,
            ExchangeState::Closed { request, .. } => *request,
        }
    }
    pub(crate) fn closed_at(&self) -> Option<u64> {
        match self.state {
            ExchangeState::Closed { at, .. } => Some(at),
            _ => None,
        }
    }
    pub(super) fn schedule_cancel(&mut self, request: CancelRequest) {
        if let ExchangeState::Working { cancellation, .. } = &mut self.state {
            cancellation.get_or_insert(request);
        }
    }
    pub(super) fn detach(&mut self) {
        if let ExchangeState::Working {
            cancellation: Some(r),
            ..
        } = &mut self.state
        {
            r.input_interrupted = true;
        }
    }
    pub(crate) fn request(&self) -> &OrderRequest {
        &self.request
    }
    pub(super) fn is_unconfirmed(&self) -> bool {
        matches!(self.visibility, Visibility::Unconfirmed(_))
    }
    pub(super) fn working_quantity(&self) -> Option<Quantity> {
        match &self.state {
            ExchangeState::Working { remaining, .. } => Some(*remaining),
            ExchangeState::Closed { .. } => None,
        }
    }

    pub(crate) fn status(&self) -> OrderStatus {
        if self.is_unconfirmed() {
            return OrderStatus::Unconfirmed;
        }
        let size = self.request.quantity().shares().micros();
        match &self.state {
            ExchangeState::Working { remaining, .. } => OrderStatus::Working {
                filled: Shares::from_micros(size - remaining.shares().micros()),
                remaining: *remaining,
            },
            ExchangeState::Closed {
                unfilled, reason, ..
            } => OrderStatus::Closed {
                filled: Shares::from_micros(size - unfilled.micros()),
                reason: *reason,
            },
        }
    }

    pub(super) fn confirm(&mut self) -> Credit {
        match std::mem::replace(&mut self.visibility, Visibility::Confirmed) {
            Visibility::Unconfirmed(credit) => credit,
            Visibility::Confirmed => Credit::default(),
        }
    }

    pub(super) fn suppress_ack(&mut self) {
        debug_assert!(!self.is_unconfirmed());
        self.visibility = Visibility::Unconfirmed(Credit::default());
    }

    fn release(&mut self, credit: Credit) -> Credit {
        match &mut self.visibility {
            Visibility::Confirmed => credit,
            Visibility::Unconfirmed(escrow) => {
                escrow.add(credit);
                Credit::default()
            }
        }
    }

    pub(super) fn held_totals(&self) -> (u128, u64) {
        let collateral = match &self.state {
            ExchangeState::Working {
                remaining,
                collateral,
                ..
            } => collateral.held(*remaining),
            ExchangeState::Closed { .. } => Credit::default(),
        };
        match &self.visibility {
            Visibility::Unconfirmed(escrow) => (
                collateral.cash + escrow.cash + escrow.rebates.total(),
                collateral.shares + escrow.shares,
            ),
            Visibility::Confirmed => (collateral.cash, collateral.shares),
        }
    }

    pub(super) fn close(&mut self, reason: CloseReason, at: u64) -> Credit {
        let Some(remaining) = self.working_quantity() else {
            return Credit::default();
        };
        let request = self.cancellation();
        let ExchangeState::Working { collateral, .. } = std::mem::replace(
            &mut self.state,
            ExchangeState::Closed {
                at,
                request,
                unfilled: remaining.shares(),
                reason,
            },
        ) else {
            unreachable!("checked working order");
        };
        self.release(collateral.held(remaining))
    }

    // Bounds: matching supplies no more than the working quantity, never an
    // execution outside the limit, and a fee below the reserved buy amount.
    pub(super) fn fill(
        &mut self,
        quantity: Quantity,
        gross: Cash,
        fee: Cash,
        rebates: AccruedRebates,
        at: u64,
    ) -> Credit {
        let ExchangeState::Working {
            remaining,
            collateral,
            cancellation,
        } = &mut self.state
        else {
            unreachable!("only working orders have queue priority");
        };
        let credit = match collateral {
            Collateral::Buy { .. } => Credit {
                cash: collateral.held(quantity).cash - gross.ticks() - fee.ticks(),
                shares: quantity.shares().micros(),
                rebates,
            },
            Collateral::Sell => Credit {
                cash: gross.ticks() - fee.ticks(),
                shares: 0,
                rebates,
            },
        };
        if let Some(rest) =
            Quantity::from_micros(remaining.shares().micros() - quantity.shares().micros())
        {
            *remaining = rest;
        } else {
            self.state = ExchangeState::Closed {
                unfilled: Shares::ZERO,
                reason: CloseReason::Filled,
                request: *cancellation,
                at,
            };
        }
        self.release(credit)
    }
}
