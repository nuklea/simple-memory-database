from lark import Lark, Transformer, v_args

grammar = r"""
start: command

?command: set_cmd
       | get_cmd
       | unset_cmd
       | counts_cmd
       | find_cmd
       | end_cmd
       | begin_cmd
       | rollback_cmd
       | commit_cmd

set_cmd: "SET" VAR value     -> set
get_cmd: "GET" VAR           -> get
unset_cmd: "UNSET" VAR       -> unset
counts_cmd: "COUNTS" value   -> counts
find_cmd: "FIND" value       -> find
end_cmd: "END"               -> end
begin_cmd: "BEGIN"           -> begin
rollback_cmd: "ROLLBACK"     -> rollback
commit_cmd: "COMMIT"         -> commit

VAR: /[A-Za-z_][A-Za-z0-9_]*/

?value: SIGNED_NUMBER        -> number
      | ESCAPED_STRING       -> string

%import common.SIGNED_NUMBER
%import common.ESCAPED_STRING
%import common.WS
%ignore WS
"""

parser = Lark(grammar, parser="lalr", propagate_positions=True, maybe_placeholders=False)


class QueryTransformer(Transformer):
    @v_args(inline=True)
    def set(self, key, value):
        return ('SET', str(key), value)

    @v_args(inline=True)
    def get(self, key):
        return 'GET', str(key)

    @v_args(inline=True)
    def unset(self, key):
        return 'UNSET', str(key)

    @v_args(inline=True)
    def counts(self, value):
        return 'COUNTS', value

    @v_args(inline=True)
    def find(self, value):
        return 'FIND', value

    def end(self, _):
        return ('END',)

    def begin(self, _):
        return ('BEGIN',)

    def rollback(self, _):
        return ('ROLLBACK',)

    def commit(self, _):
        return ('COMMIT',)

    @v_args(inline=True)
    def number(self, token):
        text = token.value
        return float(text) if '.' in text else int(text)

    @v_args(inline=True)
    def string(self, token):
        return str(token.value[1:-1])  # remove quotes


def parse(text_command):
    tree = parser.parse(text_command)
    return QueryTransformer().transform(tree)
