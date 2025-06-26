#!/usr/bin/env python3
import sys

import lark

from database import Database, SoftError

database = Database()

try:
    while True:
        sys.stdout.write('> ')
        command = input()

        try:
            result = database.execute(command)
            match result:
                case None:
                    pass
                case PrintableResult:
                    if result.message is None:
                        print('NULL')
                    else:
                        print(result.message)
        except SoftError as e:
            print(e.message)
        except lark.exceptions.UnexpectedToken as e:
            print(str(e))
except EOFError:
    print('Bye-bye!')
