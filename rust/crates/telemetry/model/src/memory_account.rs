use quent_model::{fsm, resource, state};

resource! {
    /// Bytes charged to DuckDB's buffer-pool accounting.
    BufferPoolMemory {
        resizable: true,
        capacity: { bytes: Option<u64> },
    }
}

resource! {
    /// Live buffer representations held in DuckDB temporary storage.
    TemporaryStorage {
        resizable: true,
        capacity: { bytes: Option<u64> },
    }
}

resource! {
    /// DuckDB-accounted temporary-directory extent charged to the swap limit.
    TemporaryDirectoryStorage {
        resizable: true,
        capacity: { bytes: Option<u64> },
    }
}

state! {
    AccountRegistered {
        attributes: {
            memory_tag: String,
        },
    }
}

state! {
    Accounted {
        usages: {
            buffer_pool: BufferPoolMemory,
            temporary_storage: TemporaryStorage,
            temporary_directory: TemporaryDirectoryStorage,
        },
    }
}

fsm! {
    /// One absolute occupancy gauge for a memory tag and accounting domain.
    MemoryAccount {
        states: {
            account_registered: AccountRegistered,
            accounted: Accounted,
        },
        entry: account_registered,
        exit_from: { accounted },
        transitions: {
            account_registered => accounted,
            accounted => accounted,
        },
    }
}
