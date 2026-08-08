use super::*;

impl Funnel {
    pub fn new(store: Store, clock_ms: u64) -> Result<Funnel, String> {
        wire::validate_timestamp(clock_ms)?;
        Ok(Funnel {
            store,
            gate: Mutex::new(None),
            clock_ms: Mutex::new(clock_ms),
            mint_seq: Mutex::new(0),
        })
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub(crate) fn poison_detail(&self) -> Option<String> {
        self.gate
            .lock()
            .expect("funnel gate")
            .as_ref()
            .map(|detail| format!("funnel poisoned by uncertain commit: {detail}"))
    }

    pub(crate) fn poison(&self, detail: String) {
        let mut gate = self.gate.lock().expect("funnel gate");
        if gate.is_none() {
            *gate = Some(detail);
        }
    }

    pub fn into_store(self) -> Store {
        self.store
    }

    /// Advance the logical clock (monotonic; lower values are ignored).
    pub fn tick(&self, to_ms: u64) -> Result<(), String> {
        wire::validate_timestamp(to_ms)?;
        let mut clock = self.clock_ms.lock().expect("clock");
        *clock = (*clock).max(to_ms);
        Ok(())
    }

    pub(super) fn mint_id(&self) -> Uuid7 {
        let ms = *self.clock_ms.lock().expect("clock");
        let mut seq = self.mint_seq.lock().expect("seq");
        *seq = seq.checked_add(1).expect("mint sequence exhausted");
        let entropy = (u128::from(self.store.authority_epoch().0) << 64) | u128::from(*seq);
        Uuid7::mint(ms, entropy)
    }
}
