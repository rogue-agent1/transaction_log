#!/usr/bin/env python3
"""Transaction log — ARIES-style recovery with undo/redo logging.

One file. Zero deps. Does one thing well.

Implements write-ahead logging with checkpoints, undo for uncommitted
transactions, and redo for committed ones. Core of database crash recovery.
"""
import sys, time
from enum import Enum

class LogType(Enum):
    BEGIN = "BEGIN"
    UPDATE = "UPDATE"
    COMMIT = "COMMIT"
    ABORT = "ABORT"
    CHECKPOINT = "CHECKPOINT"
    CLR = "CLR"  # Compensation Log Record

class LogRecord:
    __slots__ = ('lsn', 'txn_id', 'log_type', 'page_id', 'old_val', 'new_val', 'prev_lsn')
    _counter = 0
    def __init__(self, txn_id, log_type, page_id=None, old_val=None, new_val=None, prev_lsn=None):
        LogRecord._counter += 1
        self.lsn = LogRecord._counter
        self.txn_id = txn_id
        self.log_type = log_type
        self.page_id = page_id
        self.old_val = old_val
        self.new_val = new_val
        self.prev_lsn = prev_lsn
    def __repr__(self):
        if self.log_type == LogType.UPDATE:
            return f"[{self.lsn}] T{self.txn_id} {self.log_type.value} P{self.page_id}: {self.old_val}→{self.new_val}"
        return f"[{self.lsn}] T{self.txn_id} {self.log_type.value}"

class TransactionLog:
    def __init__(self):
        self.log = []
        self.pages = {}  # page_id -> value (simulated buffer pool)
        self.active_txns = {}  # txn_id -> last_lsn
        self.txn_counter = 0

    def begin(self):
        self.txn_counter += 1
        tid = self.txn_counter
        rec = LogRecord(tid, LogType.BEGIN)
        self.log.append(rec)
        self.active_txns[tid] = rec.lsn
        return tid

    def update(self, txn_id, page_id, new_val):
        old_val = self.pages.get(page_id)
        rec = LogRecord(txn_id, LogType.UPDATE, page_id, old_val, new_val, self.active_txns.get(txn_id))
        self.log.append(rec)
        self.active_txns[txn_id] = rec.lsn
        self.pages[page_id] = new_val  # Write to buffer
        return rec.lsn

    def commit(self, txn_id):
        rec = LogRecord(txn_id, LogType.COMMIT, prev_lsn=self.active_txns.get(txn_id))
        self.log.append(rec)
        del self.active_txns[txn_id]

    def abort(self, txn_id):
        self._undo_txn(txn_id)
        rec = LogRecord(txn_id, LogType.ABORT, prev_lsn=self.active_txns.get(txn_id))
        self.log.append(rec)
        del self.active_txns[txn_id]

    def checkpoint(self):
        rec = LogRecord(0, LogType.CHECKPOINT)
        self.log.append(rec)

    def _undo_txn(self, txn_id):
        """Undo all updates by txn_id in reverse order."""
        for rec in reversed(self.log):
            if rec.txn_id == txn_id and rec.log_type == LogType.UPDATE:
                self.pages[rec.page_id] = rec.old_val
                clr = LogRecord(txn_id, LogType.CLR, rec.page_id, rec.new_val, rec.old_val)
                self.log.append(clr)

    def crash_recover(self):
        """ARIES-style recovery: Analysis → Redo → Undo."""
        print("\n--- CRASH RECOVERY ---")
        # Phase 1: Analysis — find active txns at crash
        committed = set()
        active = set()
        for rec in self.log:
            if rec.log_type == LogType.BEGIN:
                active.add(rec.txn_id)
            elif rec.log_type == LogType.COMMIT:
                active.discard(rec.txn_id)
                committed.add(rec.txn_id)
            elif rec.log_type == LogType.ABORT:
                active.discard(rec.txn_id)
        print(f"  Analysis: committed={committed}, active(losers)={active}")

        # Phase 2: Redo — replay all updates (committed + uncommitted)
        redo_count = 0
        for rec in self.log:
            if rec.log_type == LogType.UPDATE:
                self.pages[rec.page_id] = rec.new_val
                redo_count += 1
        print(f"  Redo: replayed {redo_count} updates")

        # Phase 3: Undo — rollback uncommitted transactions
        undo_count = 0
        for rec in reversed(self.log):
            if rec.txn_id in active and rec.log_type == LogType.UPDATE:
                self.pages[rec.page_id] = rec.old_val
                undo_count += 1
        print(f"  Undo: rolled back {undo_count} updates from {len(active)} loser txn(s)")
        
        # Mark losers as aborted
        for tid in active:
            self.active_txns.pop(tid, None)

        return committed, active

def main():
    tl = TransactionLog()
    
    # Normal operations
    t1 = tl.begin()
    tl.update(t1, "page-A", 100)
    tl.update(t1, "page-B", 200)
    tl.commit(t1)

    t2 = tl.begin()
    tl.update(t2, "page-A", 300)
    tl.update(t2, "page-C", 400)
    # T2 does NOT commit — simulates crash

    t3 = tl.begin()
    tl.update(t3, "page-B", 500)
    tl.commit(t3)

    print("=== Transaction Log ===")
    for rec in tl.log:
        print(f"  {rec}")
    print(f"\nPages before crash: {tl.pages}")
    print(f"Active (uncommitted): T{t2}")

    # Crash and recover
    committed, losers = tl.crash_recover()
    print(f"\nPages after recovery: {tl.pages}")
    print(f"  page-A should be 100 (T1 committed, T2 undone): {'✓' if tl.pages.get('page-A') == 100 else '✗'}")
    print(f"  page-B should be 500 (T3 committed): {'✓' if tl.pages.get('page-B') == 500 else '✗'}")
    print(f"  page-C should be None (T2 undone): {'✓' if tl.pages.get('page-C') is None else '✗'}")

if __name__ == "__main__":
    main()
