//! Process table for tracking spawned child processes.
//!
//! Maps exec_id to process entries, providing lookup, insertion,
//! removal, and iteration for the connection handler.

use std::collections::HashMap;

use tokio::process::{Child, ChildStdin};

/// An entry in the process table for a spawned child process.
pub struct ProcessEntry {
    /// The tokio child process handle.
    pub child: Child,
    /// The child's stdin pipe (if still open).
    pub stdin: Option<ChildStdin>,
}

impl ProcessEntry {
    /// Get the OS process ID, if available.
    pub fn pid(&self) -> Option<i32> {
        self.child.id().map(|id| id as i32)
    }
}

/// Table of active child processes, keyed by exec_id.
pub struct ProcessTable {
    entries: HashMap<u64, ProcessEntry>,
}

impl ProcessTable {
    /// Create an empty process table.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Insert a new process entry.
    pub fn insert(&mut self, exec_id: u64, child: Child, stdin: Option<ChildStdin>) {
        self.entries.insert(exec_id, ProcessEntry { child, stdin });
    }

    /// Look up a process by exec_id.
    pub fn get(&self, exec_id: u64) -> Option<&ProcessEntry> {
        self.entries.get(&exec_id)
    }

    /// Look up a process mutably by exec_id.
    pub fn get_mut(&mut self, exec_id: u64) -> Option<&mut ProcessEntry> {
        self.entries.get_mut(&exec_id)
    }

    /// Remove a process from the table.
    pub fn remove(&mut self, exec_id: u64) -> Option<ProcessEntry> {
        self.entries.remove(&exec_id)
    }

    /// Check if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &ProcessEntry)> {
        self.entries.iter()
    }

    /// Iterate mutably over all entries.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&u64, &mut ProcessEntry)> {
        self.entries.iter_mut()
    }

    /// Remove all entries from the table.
    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table() {
        let table = ProcessTable::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        assert!(table.get(1).is_none());
    }
}
