use super::ChaosExchange;
use crate::paper::{
    ClientOrderId, Error, OrderKey, Shares, Stamp,
    hybrid_coordinator::{HybridPlan, Progress, Step, progress},
    order_fsm::{Delivery, OrderStatus},
};

impl ChaosExchange {
    pub(in crate::paper) fn step_hybrid(
        &mut self,
        plan: &HybridPlan,
        signal: Option<Stamp>,
        step: Step,
    ) -> Result<Progress, Error> {
        let maker = &plan.maker;
        let key = maker.account().order(maker.id());
        if let Some(committed) = self.hybrids.get(&maker.id()) {
            if committed != plan {
                return Err(Error::ConflictingOrder);
            }
            return self.resume_handoff(key, plan.fallback_id(), signal, step.submit_receipt);
        }
        if !step.escalate {
            let receipt = self.submit_at(maker.clone(), signal, step.submit_receipt)?;
            return Ok(progress(key, receipt.status(), Shares::ZERO));
        }
        let Some(receipt) = self.prior(maker)? else {
            let receipt =
                self.submit_new(plan.taker(maker.quantity()), signal, step.submit_receipt)?;
            self.hybrids.insert(maker.id(), plan.clone());
            return Ok(progress(receipt.key(), receipt.status(), Shares::ZERO));
        };
        match receipt.status() {
            OrderStatus::Working { remaining, .. } => {
                self.cancel_for(key, &plan.taker(remaining), signal, step.cancel_receipt)?;
                self.hybrids.insert(maker.id(), plan.clone());
                self.resume_handoff(key, plan.fallback_id(), signal, step.submit_receipt)
            }
            status => Ok(progress(key, status, Shares::ZERO)),
        }
    }

    fn resume_handoff(
        &mut self,
        maker: OrderKey,
        fallback: ClientOrderId,
        signal: Option<Stamp>,
        delivery: Delivery,
    ) -> Result<Progress, Error> {
        let status = self.order_status(maker);
        if self.reserved_for(fallback, maker) {
            let status = status?;
            let OrderStatus::Closed { filled, .. } = status else {
                return Ok(progress(maker, status, Shares::ZERO));
            };
            return match self.submit_reserved(fallback, signal, delivery) {
                Ok(receipt) => Ok(progress(receipt.key(), receipt.status(), filled)),
                Err(Error::InvalidValue) => {
                    self.abandon_fallback(maker)?;
                    Ok(Progress::Complete { filled })
                }
                Err(error) => Err(error),
            };
        }
        let maker_filled = match status {
            Ok(OrderStatus::Closed { filled, .. }) => filled,
            Err(Error::UnknownOrder) => Shares::ZERO, // A direct IOC skipped the maker.
            Err(err) => return Err(err),
            Ok(_) => return Err(Error::InvalidValue),
        };
        let key = maker.account.order(fallback);
        Ok(progress(key, self.order_status(key)?, maker_filled))
    }
}
