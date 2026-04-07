from typing import Sequence

from karps.config import ResourceConfig
from karps.query.query import ReadyQuery


ELEMENT_SEPARATOR = "\u001f"
FIELD_SEPARATOR = "\u001e"


class SQLQuery:
    def __init__(self, selection: Sequence[tuple[str, str]]):
        # tuple pair represents value (1) AS alias (2)
        self.selection = selection
        self.table = None
        # where
        self._op = "and"
        # each element will generate a CTE
        self.joins = []
        self.data_joins = {}
        self.where_clause = None
        self._group_by = None
        self._order_by = None
        self._from = 0
        self.size = None
        self.inner_queries = ()

    def from_table(self, tbl_name):
        self.table = tbl_name
        return self

    def from_inner_query(self, queries: Sequence[tuple[ResourceConfig | None, "SQLQuery"]]) -> "SQLQuery":
        """
        If a query is on a concrete DB table, it is accomanied by a resource config. If the query
        is on a derived table, the resource config can be None
        """
        self.inner_queries = queries
        return self

    def join(
        self,
        field,
        alias=None,
        count: int | None = None,
        where: ReadyQuery | None = None,
        field_names: list[str] | None = None,
    ):
        """
        field must be in a table with two columns, __parent_id and value
        a CTE and a join (LEFT or INNER, depending on if there is a query on <field>)
        """
        if not field_names:
            field_names = [field]
        if where:
            # TODO should alias be used here also
            self.joins.append((field, where, field_names, count))
        else:
            self.data_joins[field] = (alias, field_names)
        return self

    def group_by(self, fields: Sequence[str]):
        self._group_by = ", ".join([f"`{field}`" for field in fields])
        return self

    def order_by(self, sort):
        self._order_by = sort
        return self

    def op(self, _op):
        self._op = _op
        return self

    def where(self, clause):
        self.where_clause = clause
        return self

    def from_page(self, page):
        self._from = page
        return self

    def add_size(self, size):
        self.size = size
        return self

    def get_ctes(self, count) -> tuple[list[str], list[str]]:
        ctes = []
        params = []
        for join in self.joins:
            # query on collection field
            where = join[1]
            where_cte = None
            q_str, inner_params = (
                select([("__parent_id", None)])
                .from_table(f"{self.table}__{join[0]}")
                .where(where)
                .group_by(["__parent_id"])
                .to_string()[0]
            )

            if where:
                # join[3] == count/idx used disambiguate between mulitple clauses on the same collection field
                idx = join[3]
                where_cte = f"{join[0]}{f'_{idx}'}__where AS (" + q_str + ")"
                ctes.append(where_cte)
                params.extend(inner_params)

        if not count:
            for join_field, join in self.data_joins.items():
                if len(join[1]) > 1:
                    concat_ws = f"CONCAT_WS('{FIELD_SEPARATOR}', {','.join([f'`{inner_field_name}`' for inner_field_name in join[1]])})"
                else:
                    concat_ws = join[0] or join_field
                # TODO add table name to name of cte?
                q_str, inner_params = (
                    select(
                        [
                            ("__parent_id", None),
                            (
                                f"GROUP_CONCAT({concat_ws} ORDER BY __parent_id SEPARATOR '{ELEMENT_SEPARATOR}')",
                                join[0] or join_field,
                            ),
                        ]
                    )
                    .from_table(f"{self.table}__{join_field}")
                    .group_by(["__parent_id"])
                    .to_string()[0]
                )
                data_cte = f"{join_field}__data AS (" + q_str + ")"
                ctes.append(data_cte)
                params.extend(inner_params)
        return ctes, params

    def to_string(self, paged=False, top_level=True) -> tuple[ReadyQuery, ReadyQuery | None]:
        """
        Builds the query from the given parameters
        Returns a tuple consisting of -
        [0] - a tuple of data fetching (query, param)
        [1] - None if paged=False or a tuple of query statistics / count (query, param)
        """

        # main select stmt
        def inner(count=False) -> ReadyQuery:
            s = ""
            params = []
            if top_level:
                ctes = []
                # add needed CTE for outer query
                qs, inner_params = self.get_ctes(count=count)
                ctes.extend(qs)
                params.extend(inner_params)

                # add needed CTEs for inner queries recursively
                def recurse(q):
                    ctes = []
                    params = []
                    for _, inner_q in q.inner_queries:
                        qs, inner_params = inner_q.get_ctes(count=count)
                        ctes.extend(qs)
                        params.extend(inner_params)
                        inner_qs, inner_params = recurse(inner_q)
                        ctes.extend(inner_qs)
                        params.extend(inner_params)
                    return ctes, params

                inner_qs, inner_params = recurse(self)
                ctes.extend(inner_qs)
                params.extend(inner_params)

                if ctes:
                    s = "WITH " + ", ".join(ctes) + " "

            if count:
                selection = "COUNT(*)"
            else:
                sel = []
                for [value, alias] in self.selection:
                    # column names needs to be quoted with backticks
                    # TODO refactor, fix etc
                    if (
                        value[0] == '"'
                        or value[0] == "'"
                        or value[0:12] == "GROUP_CONCAT"
                        or value[0:5] == "COUNT"
                        or value[0:6] == "CONCAT"
                        or value[0:3] == "SUM"
                        or value[0:6] == "IFNULL"
                    ):
                        v = value
                    else:
                        v = f"`{value}`"

                    if alias:
                        sel.append(f"{v} AS {alias}")
                    else:
                        sel.append(v)
                if not sel:
                    selection = "__id"
                else:
                    selection = ", ".join(sel)
            if self.table:
                s += f"SELECT {selection} FROM `{self.table}`"
            elif self.inner_queries:
                queries: list[str] = []
                for _, inner_query in self.inner_queries:
                    q, inner_params = inner_query.to_string(top_level=False)[0]
                    queries.append(q)
                    params.extend(inner_params)
                s += f"SELECT {selection} FROM (" + " UNION ALL ".join(queries) + ") as innerq"
            else:
                raise RuntimeError("error in SQL generation")

            table_prefix = f"`{self.table}`." if self.table else ""

            # use left joins for data fetching (skip when just counting rows)
            if not count:
                # add in joins needed for data from CTE:s
                for join_field in self.data_joins:
                    # use alias or field name
                    name = self.data_joins[join_field][0] or join_field
                    s += f" LEFT JOIN `{name}__data` ON `{name}__data`.__parent_id = {table_prefix}__id"

            if self.where_clause:
                where_str, where_params = self.where_clause
                s += f" WHERE {where_str.replace('TABLE_PREFIX', table_prefix)}"
                params.extend(where_params)

            if self._group_by:
                s += f" GROUP BY {self._group_by}"

            if not count and self._order_by:
                s += " ORDER BY "
                order_bys = []
                for field, order in self._order_by:
                    order_s = f"`{field}`"
                    if order != "asc":
                        order_s += f" {order.upper()}"
                    order_bys.append(order_s)
                s += ", ".join(order_bys)

            # count queries and inner queries should not have size limits
            if not count and top_level and self.size is not None:
                s += f" LIMIT {self.size} OFFSET {self._from}"

            return s, tuple(params)

        return inner(), inner(count=True) if paged and top_level else None


def select(selection) -> SQLQuery:
    return SQLQuery(selection)
