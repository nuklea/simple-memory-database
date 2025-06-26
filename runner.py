#!/usr/bin/env python3
import sys

from database import Database

database = Database()
database.execute('SET A 10')
database.execute('SET A 11')
print(database.execute('GET A'))

try:
    while True:
        sys.stdout.write('> ')
        command = input()
        print(database.execute(command))
except EOFError:
    print('Bye-bye!')
