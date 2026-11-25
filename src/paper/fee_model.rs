//! Polymarket's per-fill taker curve and standard maker rebate tiers.
use rust_decimal::Decimal;

use super::{
    Cash, Error, Price, Shares,
    types::{SCALE, units},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MakerTier {
    None,
    Tier1,
    Tier2,
    Tier3,
    Tier4,
}
impl MakerTier {
    pub fn from_weighted_volume(volume: Cash) -> Self {
        match volume.ticks() / super::types::CASH_SCALE {
            0..200_000 => Self::None,
            200_000..1_000_000 => Self::Tier1,
            1_000_000..4_000_000 => Self::Tier2,
            4_000_000..10_000_000 => Self::Tier3,
            _ => Self::Tier4,
        }
    }
    fn percent(self) -> u128 {
        match self {
            Self::None => 0,
            Self::Tier1 => 3,
            Self::Tier2 => 7,
            Self::Tier3 => 10,
            Self::Tier4 => 15,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeeModel {
    rate: u32,
    maker_rebates: bool,
}
impl FeeModel {
    pub const FREE: Self = Self {
        rate: 0,
        maker_rebates: false,
    };

    /// Supply the market's effective rate. Set maker eligibility to false for combos.
    pub fn new(rate: Decimal, maker_rebates: bool) -> Result<Self, Error> {
        let rate = units(rate, 6)?;
        if rate > SCALE / 2 {
            return Err(Error::InvalidValue);
        }
        Ok(Self {
            rate: rate as u32,
            maker_rebates: maker_rebates && rate != 0,
        })
    }

    fn weighted_numerator(price: Price, shares: Shares) -> u128 {
        u128::from(shares.micros()) * price.micros() * (SCALE - price.micros())
    }

    pub fn maker_weighted_volume(self, price: Price, shares: Shares) -> Cash {
        Cash::from_ticks(if self.maker_rebates {
            Self::weighted_numerator(price, shares) / SCALE
        } else {
            0
        })
    }

    pub fn taker_fee(self, price: Price, shares: Shares) -> Cash {
        let raw = Self::weighted_numerator(price, shares) * u128::from(self.rate);
        let denominator = 10_000_000_000_000_000_000;
        Cash::from_ticks(((raw + denominator / 2) / denominator) * 10_000_000)
    }

    /// Per-fill accrual at the simulator's 12-decimal accounting precision.
    pub fn maker_rebate(self, tier: MakerTier, price: Price, shares: Shares) -> Cash {
        if !self.maker_rebates || self.taker_fee(price, shares) == Cash::ZERO {
            return Cash::ZERO;
        }
        let raw = Self::weighted_numerator(price, shares) * u128::from(self.rate) * tier.percent();
        Cash::from_ticks(raw / 100_000_000_000_000)
    }

    // Rounding a nonnegative amount to nearest is at most twice the raw fee.
    // This per-share-micro bound covers arbitrary partial-fill fragmentation.
    pub(super) fn buy_reserve_per_micro(self, limit: Price, taker: bool) -> u128 {
        let price = limit.micros();
        if taker {
            price + (2 * price * u128::from(self.rate)).div_ceil(SCALE)
        } else {
            price
        }
    }
}

/// Confirmed and unconfirmed accruals use the same accounting clock. This
/// keeps a fill's earning day even if its receipt is reconciled a day later.
#[derive(Default)]
pub(super) struct AccruedRebates {
    day: u64,
    current: u128,
    due: u128,
}

impl AccruedRebates {
    pub(super) fn earned(day: u64, amount: Cash) -> Self {
        Self {
            day,
            current: amount.ticks(),
            due: 0,
        }
    }

    fn advance(&mut self, day: u64) {
        if day > self.day {
            self.due += std::mem::take(&mut self.current);
            self.day = day;
        }
    }

    pub(super) fn merge(&mut self, mut other: Self) {
        if other.total() == 0 {
            return;
        }
        let day = self.day.max(other.day);
        self.advance(day);
        other.advance(day);
        self.current += other.current;
        self.due += other.due;
    }

    pub(super) fn total(&self) -> u128 {
        self.current + self.due
    }

    pub(super) fn payout(&mut self, day: u64) -> u128 {
        self.advance(day);
        if self.due < super::types::CASH_SCALE {
            return 0;
        }
        // Simulated pUSD payout has six decimal places. Keep the remainder.
        let payout = self.due / SCALE * SCALE;
        self.due -= payout;
        payout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn official_fee_curve_rounding_and_standard_rebates() {
        let fees = FeeModel::new(dec("0.05"), true).unwrap();
        let shares = Shares::new(dec("100")).unwrap();
        let price = |p| Price::new(dec(p)).unwrap();
        assert_eq!(fees.taker_fee(price("0.50"), shares).decimal(), dec("1.25"));
        assert_eq!(fees.taker_fee(price("0.30"), shares).decimal(), dec("1.05"));
        assert_eq!(fees.taker_fee(price("0.70"), shares).decimal(), dec("1.05"));
        assert_eq!(
            fees.taker_fee(price("0.01"), shares).decimal(),
            dec("0.04950")
        );
        assert_eq!(
            fees.maker_rebate(MakerTier::Tier4, price("0.30"), shares)
                .decimal(),
            dec("0.1575")
        );
        assert_eq!(
            fees.maker_weighted_volume(price("0.30"), shares).decimal(),
            dec("21")
        );
        assert_eq!(
            fees.taker_fee(price("0.01"), Shares::new(dec("0.000001")).unwrap()),
            Cash::ZERO
        );
        assert_eq!(
            FeeModel::new(dec("0.05"), false).unwrap().maker_rebate(
                MakerTier::Tier4,
                price("0.50"),
                shares
            ),
            Cash::ZERO
        );
        for (n, tier) in [
            ("199999.999999", MakerTier::None),
            ("200000", MakerTier::Tier1),
            ("1000000", MakerTier::Tier2),
            ("4000000", MakerTier::Tier3),
            ("10000000", MakerTier::Tier4),
        ] {
            assert_eq!(
                MakerTier::from_weighted_volume(Cash::new(dec(n)).unwrap()),
                tier
            );
        }
    }
}
