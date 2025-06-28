from collections import Counter
from dataclasses import dataclass
from functools import reduce
from typing import Any

from query import parse


@dataclass(frozen=True)
class PrintableResult:
    message: Any


@dataclass(frozen=True)
class SoftError(Exception):
    message: str


NotInTransactionError = SoftError('NotInTransaction')


@dataclass
class Transaction:
    change: dict
    commited: bool = False


class Database:
    def __init__(self):
        self.commited_state = {}
        self.transactions: list[Transaction] = []
        self.transaction_depth = -1

    def execute(self, text_command):
        command_tree = parse(text_command)
        command = command_tree.children[0]
        command_name = command[0]

        match command_name:
            case 'SET':
                (key, value) = command[1:]
                self._cmd_set(key, value)
            case 'GET':
                key = command[1]
                return self._cmd_get(key)
            case 'UNSET':
                key = command[1]
                self._cmd_unset(key)
            case 'COUNTS':
                value = command[1]
                return self._cmd_counts(value)
            case 'FIND':
                value = command[1]
                return self._cmd_find(value)
            case 'END':
                self._cmd_end()
            case 'BEGIN':
                self._cmd_begin()
            case 'ROLLBACK':
                self._cmd_rollback()
            case 'COMMIT':
                self._cmd_commit()

    @property
    def in_transaction(self):
        return self.transaction_depth != -1

    @property
    def last_uncommited_transaction_idx(self):
        uncommited = [i for i, t in enumerate(self.transactions) if not t.commited]
        return uncommited[-1] if uncommited else None

    def _get_state(self) -> dict:
        """Возвращает актуальное состояние, даже если фиксация не была произведена"""
        if self.in_transaction:
            return reduce(lambda state, trans: {**state, **trans.change}, self.transactions, self.commited_state)
        else:
            return self.commited_state

    def _cmd_get(self, key):
        return PrintableResult(self._get_state().get(key))

    def _cmd_set(self, key, value):
        if self.in_transaction:
            change = {key: value}
            if self.transaction_depth in self.transactions:
                self.transactions[self.transaction_depth].change.update(change)
            else:
                self.transactions.append(Transaction(change))
        else:
            self.commited_state[key] = value

    def _cmd_unset(self, key):
        if self.in_transaction:
            self.transactions.append(Transaction({key: None}))
        else:
            if key in self.commited_state:
                del self.commited_state[key]
            else:
                raise SoftError('Var {} not found'.format(key))

    def _cmd_counts(self, value):
        counter = Counter(self._get_state().values())
        return PrintableResult(counter.get(value) or 0)

    def _cmd_find(self, value):
        return PrintableResult(' '.join(k for k, v in self._get_state().items() if v == value))

    def _cmd_end(self):
        raise EOFError

    def _cmd_begin(self):
        self.transaction_depth += 1

    def _cmd_rollback(self):
        if not self.in_transaction:
            raise NotInTransactionError

        idx = self.last_uncommited_transaction_idx
        self.transactions = self.transactions[:idx]
        self.transaction_depth -= 1

    def _cmd_commit(self):
        if not self.in_transaction:
            raise NotInTransactionError

        idx = self.last_uncommited_transaction_idx
        self.transactions[idx].commited = True

        if all(t.commited for t in self.transactions):
            # Если все зафиксированы, обновляем глобальное состояние
            self.commited_state = self._get_state()
            self.transactions.clear()
            self.transaction_depth = -1
