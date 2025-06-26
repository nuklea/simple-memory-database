from collections import Counter
from dataclasses import dataclass
from functools import reduce
from typing import Any

from query_parser import parse


@dataclass
class PrintableResult:
    message: Any


@dataclass
class SoftError(Exception):
    message: str


class Database:
    def __init__(self):
        self.commited_state = {}
        self.transactions: list[dict] = []
        self.transaction_level = -1

    @property
    def in_transaction(self):
        return self.transaction_level != -1

    def _get_state(self) -> dict:
        """Возвращает актуальное состояние, даже если фиксация не была произведена"""
        if self.in_transaction:
            return reduce(lambda state, trans_ops: {**state, **trans_ops}, self.transactions, self.commited_state)
        else:
            return self.commited_state

    def _cmd_set(self, key, value):
        if self.in_transaction:
            change = {key: value}
            if self.transaction_level in self.transactions:
                self.transactions[self.transaction_level].update(change)
            else:
                self.transactions.append(change)
        else:
            self.commited_state[key] = value

    def _cmd_unset(self, key):
        if self.in_transaction:
            self.transactions.append({key: None})
        else:
            if key in self.commited_state:
                del self.commited_state[key]
            else:
                raise SoftError('Var {} not found'.format(key))

    def _cmd_counts(self, value):
        counter = Counter(self._get_state().values())
        return PrintableResult(counter.get(value) or 0)

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
                return PrintableResult(self._get_state().get(key))

            case 'UNSET':
                key = command[1]
                self._cmd_unset(key)

            case 'COUNTS':
                value = command[1]
                return self._cmd_counts(value)

            case 'FIND':
                value = command[1]
                return PrintableResult(' '.join(k for k, v in self._get_state().items() if v == value))

            case 'END':
                raise EOFError

            case 'BEGIN':
                self.transaction_level += 1

            case 'ROLLBACK':
                if not self.in_transaction:
                    raise SoftError('Not in transaction')

                self.transactions.pop()
                self.transaction_level -= 1

            case 'COMMIT':
                self.commited_state = self._get_state()
                self.transaction_level = -1
                self.transactions.clear()
