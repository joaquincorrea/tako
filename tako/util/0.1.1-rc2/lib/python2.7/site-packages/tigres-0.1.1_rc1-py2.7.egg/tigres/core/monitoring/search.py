"""
`tigres.core.monitoring.search`
================================

.. currentmodule:: tigres.core.monitoring.search

:platform: Unix, Mac
:synopsis: Query log files

..code-block::

    f = LogFile('/tmp/my.log')
    q = Query()
    cl = Clause()
    cl.add(Expr('age = 7'))
    cl.add(Expr('name ~ M(ae|ai|ay)'))
    q.add(cl)
    # could also do above in 1 line:
    # q = Query(clauses=[Clause(exprs=[Expr('age = 7'), Expr('name ~ M(ae|ai|ay)')])])
    for item in f.query(q):
     print(item)


"""
from tigres.core.date import parse
from tigres.core.monitoring import common, kvp

__author__ = 'Dan Gunter <dkgunter@lbl.gov>'
__date__ = '4/9/13'

import re
# package imports


class Query(object):
    """A query built of clauses.
    """

    def __init__(self, clauses=None):
        self._clauses = [] if clauses is None else list(clauses)

    def add(self, clause):
        self._clauses.append(clause)

    def __iter__(self):
        return iter(self._clauses)


class Clause(object):
    """Set of expressions connected by AND or OR.
    """

    def __init__(self, conjunction=True, exprs=None):
        self._and = conjunction
        self._exprs = [] if exprs is None else exprs

    def is_and(self):
        return self._and

    def add(self, expr):
        self._exprs.append(expr)

    def __iter__(self):
        return iter(self._exprs)


class Expr(object):
    """An expression in a filter, e.g. 'foo > 2'
    """
    _op_errmsg = "Invalid expression '{}', expected '<field> <operator> <value>'"

    def __init__(self, text):
        """Create expression from text.

        :param text: Input text
        :type text: str
        :raise: ValueError if not a valid expression
        """
        parts = text.split()
        if len(parts) != 3:
            raise ValueError(self._op_errmsg.format(text))
        self._field = parts[0]
        oper_str = parts[1]
        self._val = parts[2]
        # if val is a date, then parse it and optionally modify operator
        _, mdate = parse.guess(self._val, try_num=False)
        if mdate is not None:
            self._val = mdate
            if self._field != common.Keyword.TIME:
                # time will have the value itself pre-parsed, but other
                # fields will not, so we indicate this in the operator
                oper_str = Oper.DATE_VALUE_PREFIX + oper_str
        elif self._field == common.Keyword.TIME:
            raise ValueError(
                "Cannot compare non-date to timestamp field ({}): {}".format(
                    self._field, self._val))
        self._op = Oper(oper_str)

    @property
    def field(self):
        return self._field

    @property
    def op(self):
        return self._op

    @property
    def value(self):
        return self._val


class Oper(object):
    """Operator in an expression, e.g. '=' or '~'.
    """
    _opname = {'>': 'gt', '>=': 'gte',
               '<': 'lt', '<=': 'lte',
               '=': 'eq', '!=': 'neq',
               '~': 'req'}

    DATE_VALUE_PREFIX = '@'

    def __init__(self, strval):
        """Create from a string.

        :param strval: Input string
        :type strval: str
        :raise: ValueError, if unknown
        """
        self._rhs_date = False
        if strval.startswith(self.DATE_VALUE_PREFIX):
            self._rhs_date = True
            strval = strval[len(self.DATE_VALUE_PREFIX):]
        n = self._opname.get(strval, None)
        if n is None:
            raise ValueError("unknown operator '{}'".format(strval))
        self._opfn = getattr(self, '_compare_{}'.format(n))
        self._must_be_numeric = not n.endswith('eq')

    def compare(self, lhs, rhs):
        """Compare two values with this operator.

        :return: Boolean result of comparison
        :rtype: bool
        """
        if self._rhs_date:
            _, rhs_d = parse.guess(rhs)
            if rhs_d is None:
                return False  # Not a date; exception?
            rhs = rhs_d
        elif self._must_be_numeric:
            lhs = self._number(lhs)
            rhs = self._number(rhs)
            if lhs is None or rhs is None:
                return False  # Not a number; exception?
        return self._opfn(lhs, rhs)

    def _number(self, v):
        try:
            x = int(v)
        except ValueError:
            try:
                x = float(v)
            except ValueError:
                x = None
        return x

    def _compare_gt(self, lhs, rhs):
        """Compare two numeric values, A and B,
           and return whether A > B.
        """
        return lhs > rhs

    def _compare_gte(self, lhs, rhs):
        """Compare two numeric values, A and B,
           and return whether A >= B.
        """
        return lhs >= rhs

    def _compare_lt(self, lhs, rhs):
        """Compare two numeric values, A and B,
           and return whether A < B.
        """
        return lhs < rhs

    def _compare_lte(self, lhs, rhs):
        """Compare two numeric values, A and B,
           and return whether A <= B.
        """
        return lhs <= rhs

    def _compare_eq(self, lhs, rhs):
        """Compare two numeric or string values, A and B,
           and return whether A = B.
        """
        return lhs == rhs

    def _compare_neq(self, lhs, rhs):
        """Compare two numeric or string values, A and B,
            and return whether A != B.
        """
        return lhs != rhs

    def _compare_req(self, lhs, rhs):
        """Compare the string value, A, with the regular expression B,
        and return whether B matches A.
        """
        return re.match(rhs, lhs)


class Queryable(object):
    """Interface for logs we can query.
    """

    def query(self, qry):
        """Run the query.

        :param qry: The query to apply
        :type qry: Query
        :return: Matching log items
        :rtype: list of dict
        """
        pass


class LogFile(Queryable):
    """A queryable log stored in a text file.
    """

    def __init__(self, path):
        self._f = open(path)

    def query(self, qry):
        for rec in kvp.Reader(self._f):
            ok = True
            for clause in qry:
                if clause.is_and():
                    passed = all((self._match(expr, rec) for expr in clause))
                else:
                    passed = any((self._match(expr, rec) for expr in clause))
                if not passed:
                    ok = False
                    break
            if ok:
                yield rec

    def _match(self, expr, rec):
        r = False
        try:
            r = expr.field in rec and expr.op.compare(rec[expr.field],
                                                      expr.value)
        except TypeError as err:
            r = False
        return r


class LogSqlite(Queryable):
    """A queryable log stored in an sqlite database.
    """

    def __init__(self, path):
        pass


def expr_documentation(as_str=False):
    """Utility function to get documentation for
    all the operators.

    If as_str is False, returns a dictionary, keyed by the operator, with values
    being a description of that operator.
    If as_str is True, returns a string containing one line of description per
    operator.
    """
    import re

    ops = {}
    for op, opfn in Oper._opname.items():
        func = "_compare_{}".format(opfn)
        docstr = getattr(Oper, func).__doc__.replace('\n', ' ')
        docstr = re.sub(r' +', ' ', docstr)  # collapse repeated spaces
        ops[op] = docstr
    if as_str:
        strops = ["{:6s} {}".format(k, v) for k, v in ops.items()]
        return '\n'.join(strops)
    else:
        return ops
