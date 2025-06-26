from collections import Counter
from functools import reduce

from command_parser import parse


class Database:
    def __init__(self):
        self.commited_state = {}
        self.transaction: list[dict] = []
        self.transaction_level = -1

    @property
    def in_transaction(self):
        return self.transaction_level != -1

    def _get_state(self) -> dict:
        """Возвращает актуальное состояние, даже если фиксация не была произведена"""
        if self.in_transaction:
            return reduce(lambda state, trans_ops: {**state, **trans_ops}, self.transaction, self.commited_state)
        else:
            return self.commited_state

    def _cmd_set(self, key, value):
        if self.in_transaction:
            change = {key: value}
            if self.transaction_level in self.transaction:
                self.transaction[self.transaction_level].update(change)
            else:
                self.transaction.append(change)
        else:
            self.commited_state[key] = value

    def _cmd_unset(self, key):
        if self.in_transaction:
            self.transaction.append({key: None})
        else:
            if key in self.commited_state:
                del self.commited_state[key]
            else:
                return 'Var {} not found'.format(key)

    def _cmd_counts(self, value):
        counter = Counter(self._get_state().values())
        return counter.get(value) or 0

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
                return self._get_state().get(key)

            case 'UNSET':
                key = command[1]
                self._cmd_unset(key)

            case 'COUNTS':
                value = command[1]
                return self._cmd_counts(value)

            case 'FIND':
                value = command[1]
                return ' '.join(k for k, v in self._get_state().items() if v == value)

            case 'END':
                raise EOFError

            case 'BEGIN':
                self.transaction_level += 1

            case 'ROLLBACK':
                self.transaction.pop()
                self.transaction_level -= 1

            case 'COMMIT':
                self.commited_state = self._get_state()
                self.transaction_level = -1
                self.transaction.clear()
