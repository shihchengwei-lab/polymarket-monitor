//! Paper matching with synchronized live depth or ordered replay data.
//!
//! Fund disjoint strategy accounts before admitting the first book observation.
//! Attach a funded exchange to a live group with [`crate::book::NegRiskEventGroup::attach_paper`],
//! or drive it offline using a generation from `feed`. In offline mode advance the
//! logical clock and supply explicit ticks and ordered events. Retiring a feed
//! discards its priority and rejects delayed inputs from that generation. Only explicit
//! trades advance resting order fills. Public L2 has aggregate quantities,
//! so FIFO is exact within this simulator's queue assumptions: new depth joins
//! the back and unspecified cancellations remove external quantity from the
//! back first. It cannot identify an individual order's position at the venue.
//!
//! A print spends one quantity across executable prices in price-time order.
//! Paper fills use the resting order price. The recorded public depletion
//! applies at the reported trade price.
//!
//! For an identified print and the depth reporting that same execution use
//! `ExecutionPair::try_new(trade, depth)`, then pass the validated pair to
//! `observe_execution`. When the preceding depth has aged out, the pair retires
//! the known public quantity and installs the fresh depth without assigning
//! a fill to an old paper order. A pair can also initialize a new feed.
//! Standalone `trade` and `observe`
//! require known causal order. For a gap or ambiguous order retire the feed
//! with `invalidate_depth` and start with a fresh full depth. The live group
//! delivers depth only and never infers maker fills from a decrease.
//!
//! Live depth ages from its local reception time, including time in the queue.
//! A hybrid cancellation reserves the fallback ID and collateral until the
//! outcome is known. After confirmation, a fresh signal can execute the IOC,
//! or `abandon_fallback` can release it even when the market is unavailable.
//!
//! # Example
//!
//! ```
//! use polymarket_monitor::paper::{
//!     AssetId, Cash, ClientOrderId, Price, Quantity, Shares, Side,
//!     chaos_exchange::{ChaosExchange, Depth},
//!     fee_model::{FeeModel, MakerTier},
//!     order_fsm::{CloseReason, Delivery, OrderMode, OrderRequest, OrderStatus},
//! };
//! use rust_decimal::Decimal;
//!
//! let asset = AssetId::new("42")?;
//! let funded = Cash::new(Decimal::TEN)?;
//! let mut exchange = ChaosExchange::new(100, funded)?;
//! exchange.register_asset(asset.clone(), FeeModel::FREE, Shares::ZERO)?;
//! let account = exchange.allocate(funded, [], MakerTier::None)?;
//! let stamp = exchange.feed(&asset)?.stamp(1, 0);
//! let price = Price::new(Decimal::new(50, 2))?;
//! let size = Quantity::new(Decimal::ONE)?;
//! exchange.observe(Depth::new(asset.clone(), stamp, price, [], [(price, size)])?)?;
//! let id = ClientOrderId::new(1)?;
//! let request = OrderRequest::new(id, account, asset, Side::Buy, price, size, OrderMode::ImmediateOrCancel);
//! let receipt = exchange.submit(request, stamp, Delivery::Lost)?;
//! assert_eq!(receipt.status(), OrderStatus::Unconfirmed);
//! assert_eq!(exchange.reconcile(receipt.key())?, OrderStatus::Closed {
//!     filled: size.shares(), reason: CloseReason::Filled
//! });
//! assert!(exchange.is_conserved());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Type boundaries
//!
//! A price outside its validated range cannot be constructed directly.
//!
//! ```compile_fail
//! use polymarket_monitor::paper::Price;
//! let _ = Price(1_000_000);
//! ```
//!
//! A caller cannot change the side of a signed-off request.
//!
//! ```compile_fail
//! use polymarket_monitor::paper::{Side, order_fsm::OrderRequest};
//! fn replace_side(mut request: OrderRequest) { request.side = Side::Sell; }
//! ```
//!
//! Only the exchange can close a resting order as filled.
//!
//! ```compile_fail,E0603
//! use polymarket_monitor::paper::order_fsm::{CloseReason, Order};
//! fn mark_filled(order: &mut Order, at: u64) { order.close(CloseReason::Filled, at); }
//! ```
//!
//! Account handles come from allocation against the common capital pool.
//!
//! ```compile_fail
//! use polymarket_monitor::paper::AccountId;
//! let _ = AccountId { slot: 0, instance: 1 };
//! ```
/// Maximum simulated one-way wire delay in the live paper console.
pub const MAX_PAPER_LATENCY_MS: u64 = 300;

pub mod chaos_exchange;
pub mod fee_model;
pub mod hybrid_coordinator;
pub mod order_fsm;
mod types;

pub use types::{
    AccountId, AssetId, Cash, ClientOrderId, Error, Feed, OrderKey, Price, Quantity, Shares, Side,
    Stamp,
};
