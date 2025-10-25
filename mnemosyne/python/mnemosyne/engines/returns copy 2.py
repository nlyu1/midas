import polars as pl
import polars.selectors as cs
from atlas import printv_lazy


class ReturnsEngine:
    def __init__(self, 
            db: pl.LazyFrame, 
            backend_fair_expr: pl.Expr = pl.col('vwap_total_by_base'), 
            backend_time_expr: pl.Expr = pl.col('last_trade_time'), 
    ):
        backend_schema = db.collect_schema()
        if not (
            'time' in backend_schema and 
            'symbol' in backend_schema and 
            isinstance(backend_schema['symbol'], pl.Enum)
        ):
            raise RuntimeError("Expected input to contain time and symbol(Enum) columns")

        # We only need 'date', 'symbol', 'tick_time', 'fair' from the database
        self.db = db.select(
            'date', 'symbol', 
            backend_time_expr.alias('tick_time'),
            backend_fair_expr.alias('fair')
        )
        self.db_symbol_enum: pl.Enum = backend_schema['symbol']

    def universe_symbols(self) -> pl.Series:
        return self.db_symbol_enum.categories.sort()

    def query(self, 
            query_lf: pl.LazyFrame, 
            start_time_expr: pl.Expr = pl.col('start_time'), 
            mark_duration: pl.Expr = pl.lit('10m'), 
            tick_lag_tolerance: pl.Expr = pl.lit('30s'), 
            append_query_tick_times: bool = False, 
            append_lag: bool = True,
            filter_by_query_dates: bool = True, 
            append_start_end_fairs: bool = False,
            verbose_debug: bool = False 
        ) -> pl.LazyFrame:
        """

        Nan behavior:
            - If tick is too laggy behind query
            - If symbol is not supported in backend universe

        Appended columns:
            - `tick_to_query_lag`: maximum lag from tick to specified query time, over start and end
            - `returns`: 
        """

        query_schema = query_lf.collect_schema() 
        if not (
            'symbol' in query_schema and 
            isinstance(query_schema['symbol'], pl.Enum)
        ):
            raise RuntimeError("Expected query schema symbol(Enum)")

        query_lf_withidx = query_lf.with_row_index('row_id')
        printv_lazy(lambda: f'Query shape: {query_lf_withidx.collect().shape}', verbose_debug)

        # Append row_id and compute end_time for queries 
        # IMPORTANT: filter to compatible symbols (rest are marked with nans)
        # Then convert from querys' to backends' enum type
        # From now on, query-lf only contains backend-compatible symbols. Nans are restored upon later join
        query_with_both = (
            query_lf_withidx.select('symbol', 'row_id', start_time_expr.alias('start_time'))
            .sort(['symbol', 'start_time'])
            .filter(pl.col('symbol').cast(pl.String).is_in(list(self.db_symbol_enum.categories)))
            .with_columns(pl.col('symbol').cast(pl.String).cast(self.db_symbol_enum))
            .with_columns(
                pl.col('start_time').dt.offset_by(mark_duration).alias('end_time')
            )
            .with_columns([
                pl.col('start_time').set_sorted(),
                pl.col('end_time').set_sorted()
            ])
        )
        printv_lazy(lambda: f'Query_with_both: {query_with_both.collect().shape}, {query_with_both.collect_schema()}', verbose_debug)

        if filter_by_query_dates:
            # Select a slice of the database containing these dates
            # Sort by tick_time
            min_date, max_date = query_lf.select(
                start_time_expr.dt.date().min().alias('min'),
                start_time_expr.dt.offset_by(mark_duration).dt.date().max().alias('max')
            ).collect().row(0)
            printv_lazy(lambda: f'Query min_date: {min_date} max_date: {max_date}', verbose_debug)
            inrange_db = self.db.filter(
                pl.col('date').is_between(min_date, max_date))
        else:
            inrange_db = self.db
        inrange_db = (
            inrange_db
            .drop('date')
            .sort('symbol', 'tick_time')
            .with_columns(pl.col('tick_time').set_sorted())
        )
        printv_lazy(lambda: f'inrange_db: {inrange_db.collect_schema()}', verbose_debug)
        printv_lazy(lambda: f'Query min_date: {min_date} max_date: {max_date}', verbose_debug)
        printv_lazy(
            lambda: f"Database dates:\n{self.db.select(pl.col('tick_time').min().alias('min'), pl.col('tick_time').max().alias('max')).collect()}"
            , verbose_debug)
        printv_lazy(
            lambda: f"Filtered database dates:\n{inrange_db.select(pl.col('tick_time').min().alias('min'), pl.col('tick_time').max().alias('max')).collect()}"
            , verbose_debug)

        # Concat to a single query and sort by query_time 
        # row_id, symbol, query_time, query_type
        query_type_enum = pl.Enum(['start', 'end'])
        long_query = pl.concat([
                query_with_both.select(
                    'symbol', 'row_id', 
                    pl.col('start_time').alias('query_time'),
                    pl.lit('start').alias('query_type').cast(query_type_enum)
                ),
                query_with_both.select(
                    'symbol', 'row_id', 
                    pl.col('end_time').alias('query_time'),
                    pl.lit('end').alias('query_type').cast(query_type_enum)
                )
            ]).sort(['symbol', 'query_time'])
        printv_lazy(lambda: f'long_query: {long_query.collect().shape}, {long_query.collect_schema()}', verbose_debug)

        # Do the join-asof 
        # When tick-time is behind query_time beyond specified tolerance, fill with nan
        joined_lf = long_query.join_asof(
            inrange_db,
            left_on='query_time',
            right_on='tick_time',
            by='symbol',
            strategy='backward'
        ).with_columns(
            (pl.col('query_time') - pl.col('tick_time')).alias('tick_to_query_lag'),
            pl.when(
                pl.col('tick_time').dt.offset_by(tick_lag_tolerance) >= pl.col('query_time')
            ).then(pl.col('fair')).otherwise(None).alias('fair')
        ).select('row_id', 'symbol', 'query_time', 'query_type', 'tick_to_query_lag', 'fair', 'tick_time')
        # return query_with_both, long_query, inrange_db, joined_lf 
        printv_lazy(lambda: f'joined_lf: {joined_lf.collect().shape}, {joined_lf.collect_schema()}', verbose_debug)

        def append_columns():
            if append_query_tick_times:
                yield pl.col('query_time').filter(pl.col('query_type') == 'start').first().alias('start_query_time') 
                yield pl.col('query_time').filter(pl.col('query_type') == 'end').first().alias('end_query_time') 
                yield pl.col('tick_time').filter(pl.col('query_type') == 'start').first().alias('start_tick_time')
                yield pl.col('tick_time').filter(pl.col('query_type') == 'end').first().alias('end_tick_time')
            if append_lag:
                yield pl.col('tick_to_query_lag').max().alias('max_tick_to_query_lag')
            if append_start_end_fairs:
                yield pl.col('fair').filter(pl.col('query_type') == 'start').first().alias('start_fair')
                yield pl.col('fair').filter(pl.col('query_type') == 'end').first().alias('end_fair')
            yield (
                    (pl.col('fair').filter(pl.col('query_type') == 'end').first() - 
                    pl.col('fair').filter(pl.col('query_type') == 'start').first()) 
                    / pl.col('fair').filter(pl.col('query_type') == 'start').first()
                ).alias('return')

        # Compute the columns to append
        append_cols = (
            joined_lf.group_by(['row_id', 'symbol'])
            .agg(append_columns())
            .sort('row_id')
        ).drop('symbol')
        printv_lazy(lambda: f'append_cols: {append_cols.collect().shape}, {append_cols.collect_schema()}', verbose_debug)

        result = query_lf_withidx.join(
            append_cols, on='row_id', 
            how='left'
        ) # .drop('row_id')

        return result

    def query_batch(self,
            query_lf: pl.LazyFrame,
            mark_exprs: dict[str, tuple[pl.Expr, pl.Expr]],
            tick_lag_tolerance: pl.Expr = pl.lit('30s'),
            append_query_tick_times: bool = False,
            append_lag: bool = True,
            filter_by_query_dates: bool = True,
            append_start_end_fairs: bool = False,
            verbose_debug: bool = False
        ) -> pl.LazyFrame:
        """
        Batch query for computing multiple return specifications in a single pass.

        Args:
            query_lf: Input query LazyFrame
            mark_exprs: Dictionary mapping return column names to (start_time_expr, mark_duration) tuples
                       Example: {'now_to_p10m': (pl.col('time'), pl.lit('10m')),
                                 'p1m_to_p11m': (pl.col('time').dt.offset_by('1m'), pl.lit('10m'))}
            tick_lag_tolerance: Maximum allowed lag between tick and query time
            append_query_tick_times: Whether to append query and tick times for each mark
            append_lag: Whether to append max_tick_to_query_lag for each mark
            filter_by_query_dates: Whether to filter database by query date range
            append_start_end_fairs: Whether to append start and end fair prices for each mark
            verbose_debug: Whether to print debug information

        Returns:
            LazyFrame with original columns plus (using {metric}_{ret_col_name} naming):
            - return_{ret_col_name}: return for each mark spec (e.g., return_now_to_p10m)
            - max_tick_to_query_lag_{ret_col_name}: lag for each mark (if append_lag=True)
            - start_query_time_{ret_col_name}, end_query_time_{ret_col_name}, etc. (if append_query_tick_times=True)
            - start_fair_{ret_col_name}, end_fair_{ret_col_name} (if append_start_end_fairs=True)

        Nan behavior:
            - If tick is too laggy behind query
            - If symbol is not supported in backend universe
        """

        if not mark_exprs:
            raise ValueError("mark_exprs must contain at least one mark specification")

        query_schema = query_lf.collect_schema()
        if not (
            'symbol' in query_schema and
            isinstance(query_schema['symbol'], pl.Enum)
        ):
            raise RuntimeError("Expected query schema symbol(Enum)")

        query_lf_withidx = query_lf.with_row_index('row_id')
        printv_lazy(lambda: f'Query shape: {query_lf_withidx.collect().shape}', verbose_debug)

        # Build query_with_both for all mark specs
        # Each mark spec gets its own rows tagged with return_col_name
        return_col_enum = pl.Enum(list(mark_exprs.keys()))

        frames = []
        for ret_col_name, (start_expr, duration_expr) in mark_exprs.items():
            frame = (
                query_lf_withidx
                .select('symbol', 'row_id', start_expr.alias('start_time'))
                .sort(['symbol', 'start_time'])
                .filter(pl.col('symbol').cast(pl.String).is_in(list(self.db_symbol_enum.categories)))
                .with_columns(pl.col('symbol').cast(pl.String).cast(self.db_symbol_enum))
                .with_columns(
                    pl.col('start_time').dt.offset_by(duration_expr).alias('end_time'),
                    pl.lit(ret_col_name).cast(return_col_enum).alias('return_col_name')
                )
                .with_columns([
                    pl.col('start_time').set_sorted(),
                    pl.col('end_time').set_sorted()
                ])
            )
            frames.append(frame)

        query_with_both = pl.concat(frames)
        printv_lazy(lambda: f'Query_with_both: {query_with_both.collect().shape}, {query_with_both.collect_schema()}', verbose_debug)

        if filter_by_query_dates:
            # Compute date range across all mark specs
            all_min_dates = []
            all_max_dates = []
            for start_expr, duration_expr in mark_exprs.values():
                min_d, max_d = query_lf.select(
                    start_expr.dt.date().min().alias('min'),
                    start_expr.dt.offset_by(duration_expr).dt.date().max().alias('max')
                ).collect().row(0)
                all_min_dates.append(min_d)
                all_max_dates.append(max_d)

            min_date = min(all_min_dates)
            max_date = max(all_max_dates)

            printv_lazy(lambda: f'Query min_date: {min_date} max_date: {max_date}', verbose_debug)
            inrange_db = self.db.filter(
                pl.col('date').is_between(min_date, max_date))
        else:
            inrange_db = self.db

        inrange_db = (
            inrange_db
            .drop('date')
            .sort('symbol', 'tick_time')
            .with_columns(pl.col('tick_time').set_sorted())
        )
        printv_lazy(lambda: f'inrange_db: {inrange_db.collect_schema()}', verbose_debug)
        printv_lazy(
            lambda: f"Database dates:\n{self.db.select(pl.col('tick_time').min().alias('min'), pl.col('tick_time').max().alias('max')).collect()}"
            , verbose_debug)
        printv_lazy(
            lambda: f"Filtered database dates:\n{inrange_db.select(pl.col('tick_time').min().alias('min'), pl.col('tick_time').max().alias('max')).collect()}"
            , verbose_debug)

        # Concat to a single long query with return_col_name dimension
        query_type_enum = pl.Enum(['start', 'end'])
        long_query = pl.concat([
                query_with_both.select(
                    'symbol', 'row_id', 'return_col_name',
                    pl.col('start_time').alias('query_time'),
                    pl.lit('start').alias('query_type').cast(query_type_enum)
                ),
                query_with_both.select(
                    'symbol', 'row_id', 'return_col_name',
                    pl.col('end_time').alias('query_time'),
                    pl.lit('end').alias('query_type').cast(query_type_enum)
                )
            ]).sort(['symbol', 'return_col_name', 'query_time'])
        printv_lazy(lambda: f'long_query: {long_query.collect().shape}, {long_query.collect_schema()}', verbose_debug)

        # Single join_asof for all marks - return_col_name just rides along
        joined_lf = long_query.join_asof(
            inrange_db,
            left_on='query_time',
            right_on='tick_time',
            by='symbol',
            strategy='backward'
        ).with_columns(
            (pl.col('query_time') - pl.col('tick_time')).alias('tick_to_query_lag'),
            pl.when(
                pl.col('tick_time').dt.offset_by(tick_lag_tolerance) >= pl.col('query_time')
            ).then(pl.col('fair')).otherwise(None).alias('fair')
        ).select('row_id', 'symbol', 'return_col_name', 'query_time', 'query_type',
                 'tick_to_query_lag', 'fair', 'tick_time')
        printv_lazy(lambda: f'joined_lf: {joined_lf.collect().shape}, {joined_lf.collect_schema()}', verbose_debug)

        # Define columns to append (same logic as query(), but grouped by return_col_name too)
        def append_columns():
            if append_query_tick_times:
                yield pl.col('query_time').filter(pl.col('query_type') == 'start').first().alias('start_query_time')
                yield pl.col('query_time').filter(pl.col('query_type') == 'end').first().alias('end_query_time')
                yield pl.col('tick_time').filter(pl.col('query_type') == 'start').first().alias('start_tick_time')
                yield pl.col('tick_time').filter(pl.col('query_type') == 'end').first().alias('end_tick_time')
            if append_lag:
                yield pl.col('tick_to_query_lag').max().alias('max_tick_to_query_lag')
            if append_start_end_fairs:
                yield pl.col('fair').filter(pl.col('query_type') == 'start').first().alias('start_fair')
                yield pl.col('fair').filter(pl.col('query_type') == 'end').first().alias('end_fair')
            yield (
                    (pl.col('fair').filter(pl.col('query_type') == 'end').first() -
                    pl.col('fair').filter(pl.col('query_type') == 'start').first())
                    / pl.col('fair').filter(pl.col('query_type') == 'start').first()
                ).alias('return')

        # Group by row_id, symbol, AND return_col_name
        append_cols = (
            joined_lf.group_by(['row_id', 'symbol', 'return_col_name'])
            .agg(append_columns())
            .sort('row_id', 'return_col_name')
        ).drop('symbol')
        printv_lazy(lambda: f'append_cols: {append_cols.collect().shape}, {append_cols.collect_schema()}', verbose_debug)

        # Pivot to wide format: filter by return_col_name, add suffix, then join (stays lazy!)
        schema = append_cols.collect_schema()
        metric_cols = [c for c in schema.names() if c not in ['row_id', 'return_col_name']]

        # Cast enum to string for consistent filtering
        append_cols_str = append_cols.with_columns(
            pl.col('return_col_name').cast(pl.String)
        )
        return mark_exprs, append_cols, query_lf_withidx

        # For each return_col_name: filter, rename columns with suffix, join
        wide_append_cols = None
        for ret_col_name in mark_exprs.keys():
            filtered = (
                append_cols_str
                .filter(pl.col('return_col_name') == ret_col_name)
                .select(
                    'row_id',
                    *[pl.col(col).alias(f'{col}_{ret_col_name}') for col in metric_cols]
                )
            )

            if wide_append_cols is None:
                wide_append_cols = filtered
            else:
                wide_append_cols = wide_append_cols.join(filtered, on='row_id', how='left')

        printv_lazy(lambda: f'wide_append_cols: {wide_append_cols.collect().shape}, {wide_append_cols.collect_schema()}', verbose_debug)


        # Join back to original query
        result = query_lf_withidx.join(
            wide_append_cols, on='row_id',
            how='left'
        ).drop('row_id')
        return result 

        # wide_append_cols = None
        # for ret_col_name in mark_exprs.keys():
        #     filtered = append_cols.filter(pl.col('return_col_name') == ret_col_name)
        #     filtered = filtered.select(
        #         'row_id',
        #         *[pl.col(col).alias(f'{col}_{ret_col_name}') for col in metric_cols]
        #     )

        #     if wide_append_cols is None:
        #         wide_append_cols = filtered
        #     else:
        #         wide_append_cols = wide_append_cols.join(filtered, on='row_id', how='left')


        # # Join back to original query
        # result = query_lf_withidx.join(
        #     wide_append_cols, on='row_id',
        #     how='left'
        # ).drop('row_id')
