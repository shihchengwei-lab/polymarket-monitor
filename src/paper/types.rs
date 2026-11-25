use std::{fmt, num::NonZeroU64};

use rust_decimal::Decimal;

pub(super) const SCALE: u128 = 1_000_000;
pub(super) const CASH_SCALE: u128 = SCALE * SCALE;
pub(super) const MAX_SHARES: u64 = 1_000_000_000_000;
pub(super) const MAX_CASH: u128 = 1_000_000_000 * CASH_SCALE;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidValue,
    UnknownAccount,
    UnknownAsset,
    UnknownOrder,
    Capacity,
    InsufficientFunds,
    ConflictingOrder,
    StaleSignal,
    Unconfirmed,
    InvalidDepth,
    AlreadyStarted,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}

pub(super) fn units(value: Decimal, scale: u32) -> Result<u128, Error> {
    if value < Decimal::ZERO {
        return Err(Error::InvalidValue);
    }
    let value = value.normalize();
    if value.scale() > scale {
        return Err(Error::InvalidValue);
    }
    (value.mantissa() as u128)
        .checked_mul(10_u128.pow(scale - value.scale()))
        .ok_or(Error::InvalidValue)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cash(u128);

impl Cash {
    pub const ZERO: Self = Self(0);
    pub fn new(value: Decimal) -> Result<Self, Error> {
        let amount = units(value, 12)?;
        if amount <= MAX_CASH {
            Ok(Self(amount))
        } else {
            Err(Error::InvalidValue)
        }
    }
    pub fn decimal(self) -> Decimal {
        Decimal::from_i128_with_scale(self.0 as i128, 12).normalize()
    }
    pub(super) fn ticks(self) -> u128 {
        self.0
    }
    pub(super) fn from_ticks(ticks: u128) -> Self {
        Self(ticks)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Shares(u64);

impl Shares {
    pub const ZERO: Self = Self(0);
    pub fn new(value: Decimal) -> Result<Self, Error> {
        let amount = units(value, 6)?;
        if amount <= MAX_SHARES as u128 {
            Ok(Self(amount as u64))
        } else {
            Err(Error::InvalidValue)
        }
    }
    pub fn decimal(self) -> Decimal {
        Decimal::from_i128_with_scale(i128::from(self.0), 6).normalize()
    }
    pub(super) fn micros(self) -> u64 {
        self.0
    }
    pub(super) fn from_micros(micros: u64) -> Self {
        Self(micros)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Quantity(NonZeroU64);
impl Quantity {
    pub fn new(value: Decimal) -> Result<Self, Error> {
        Self::from_micros(Shares::new(value)?.0).ok_or(Error::InvalidValue)
    }
    pub fn shares(self) -> Shares {
        Shares(self.0.get())
    }
    pub(super) fn from_micros(micros: u64) -> Option<Self> {
        NonZeroU64::new(micros).map(Self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Price(u32);
impl Price {
    pub fn new(value: Decimal) -> Result<Self, Error> {
        let price = units(value, 6)?;
        if (1..SCALE).contains(&price) {
            Ok(Self(price as u32))
        } else {
            Err(Error::InvalidValue)
        }
    }
    pub fn decimal(self) -> Decimal {
        Decimal::new(i64::from(self.0), 6).normalize()
    }
    pub(super) fn micros(self) -> u128 {
        u128::from(self.0)
    }
    pub fn value(self, shares: Shares) -> Cash {
        Cash(self.micros() * u128::from(shares.0))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ClientOrderId(NonZeroU64);
impl ClientOrderId {
    pub fn new(value: u64) -> Result<Self, Error> {
        NonZeroU64::new(value).map(Self).ok_or(Error::InvalidValue)
    }
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AccountId {
    pub(super) slot: usize,
    pub(super) instance: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderKey {
    pub(super) account: AccountId,
    pub(super) id: ClientOrderId,
}
impl OrderKey {
    pub fn client_order_id(self) -> ClientOrderId {
        self.id
    }
}
impl AccountId {
    pub fn order(self, id: ClientOrderId) -> OrderKey {
        OrderKey { account: self, id }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AssetId(String);
impl AssetId {
    pub fn new(value: &str) -> Result<Self, Error> {
        if value.is_empty() || value.len() > 78 || !value.bytes().all(|b| b.is_ascii_digit()) {
            return Err(Error::InvalidValue);
        }
        Ok(Self(value.to_owned()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}
impl Side {
    pub(super) fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
    pub(super) fn crosses(self, limit: Price, price: Price) -> bool {
        match self {
            Self::Buy => price <= limit,
            Self::Sell => price >= limit,
        }
    }
}

/// Sequence assigned by the replay source and local observation time in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    feed: Feed,
    sequence: u64,
    pub(super) observed: std::time::Duration,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Feed {
    pub(super) instance: u64,
    pub(super) slot: usize,
    pub(super) generation: u64,
}
impl Feed {
    pub fn stamp(self, sequence: u64, observed_ms: u64) -> Stamp {
        self.stamp_at(sequence, std::time::Duration::from_millis(observed_ms))
    }
    pub(super) fn stamp_at(self, sequence: u64, observed: std::time::Duration) -> Stamp {
        Stamp {
            feed: self,
            sequence,
            observed,
        }
    }
}
impl Stamp {
    pub(super) fn belongs_to(self, feed: Feed) -> bool {
        self.feed == feed
    }
    pub fn sequence(self) -> u64 {
        self.sequence
    }
    pub fn observed_ms(self) -> u64 {
        self.observed.as_millis() as u64
    }
}
