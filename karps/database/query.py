from typing import Sequence

from karps.config import ResourceConfig


class SQLQuery:
    def __init__(self, selection: Sequence[tuple[str, str]]):
        # tuple pair represents value (1) AS alias (2)
        self.selection = selection
        self.table = None
        # where
        self._op = "and"
        self.clauses = []
        # each element will generate a CTE and a JOIN
        self.joins = {}
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

    def join(self, field, alias=None, where: str | None = None):
        """
        field must be in a table with two columns, __parent_id and value
        a CTE and a join (LEFT or INNER, depending on if there is a query on <field>)
        """
        self.joins[field] = (alias, where)
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
        self.clauses.append(clause)
        return self

    def from_page(self, page):
        self._from = page
        return self

    def add_size(self, size):
        self.size = size
        return self

    def get_ctes(self, join) -> tuple[str | None, str | None]:
        where = self.joins[join][1]
        where_cte = None
        if where:
            where_cte = (
                f"{join}__where AS ("
                + (
                    select([("__parent_id", None)])
                    .from_table(f"{self.table}__{join}")
                    .where(where)
                    .group_by(["__parent_id"])
                    .to_string()[0]
                )
                + ")"
            )

        data_cte = (
            f"{join}__data AS ("
            + (
                select([("__parent_id", None), ("GROUP_CONCAT(value SEPARATOR '\u001f')", self.joins[join][0] or join)])
                .from_table(f"{self.table}__{join}")
                .group_by(["__parent_id"])
                .to_string()[0]
            )
            + ")"
        )
        return where_cte, data_cte

    def to_string(self, paged=False, top_level=True) -> tuple[str, str | None]:
        """
        builds the query from the given parameters
        """

        # main select stmt
        def inner(count=False) -> str:
            s = ""
            if top_level:
                str_ctes = []
                ctes = []
                # add needed CTE for outer query
                for join in self.joins:
                    qs = self.get_ctes(join)
                    ctes.append((join, qs))

                # add needed CTEs for inner queries recursively
                def recurse(q):
                    ctes = []
                    for _, inner_q in q.inner_queries:
                        for join in inner_q.joins:
                            qs = inner_q.get_ctes(join)
                            ctes.append((join, qs))
                        ctes.extend(recurse(inner_q))
                    return ctes

                ctes.extend(recurse(self))

                for _, (where_cte, data_cte) in ctes:
                    # query on collection field
                    if where_cte:
                        str_ctes.append(where_cte)
                    # data fetching does not need to be done when counting
                    if not count:
                        str_ctes.append(data_cte)
                if str_ctes:
                    s = "WITH " + ", ".join(str_ctes) + " "

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
                s += (
                    f"SELECT {selection} FROM ("
                    + " UNION ALL ".join([s.to_string(top_level=False)[0] for _, s in self.inner_queries])
                    + ") as innerq"
                )
            else:
                raise RuntimeError("error in SQL generation")

            # for certain queries we are working against derived tables
            if self.joins:
                # for join in inner_q.joins:
                #     qs = inner_q.get_ctes(join)
                #     ctes.append((join, qs))

                table_prefix = f"{self.table}." if self.table else ""
                for join_field in self.joins:
                    # use alias or field name
                    name = self.joins[join_field][0] or join_field
                    if self.joins[join_field][1]:
                        self.clauses.append(
                            f"EXISTS (SELECT 1 FROM `{name}__where` WHERE {table_prefix}__id = __parent_id)"
                        )
                        # s += f" JOIN `{name}__where` ON `{name}__where`.__parent_id = {table_prefix}__id"

                    # use left joins for data fetching (skip when just counting rows)
                    if not count:
                        s += f" LEFT JOIN `{name}__data` ON `{name}__data`.__parent_id = {table_prefix}__id"

            # where for queries on data in columns, not joins
            if self.clauses:
                s += f" WHERE {f' {self._op} '.join(self.clauses)}"

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

            return s

        return inner(), inner(count=True) if paged and top_level else None


def select(selection) -> SQLQuery:
    return SQLQuery(selection)
