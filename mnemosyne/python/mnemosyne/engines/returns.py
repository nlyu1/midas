import polars as pl 


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
            tick_lag_tolerance: pl.Expr = pl.lit('30s')
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
        # print(query_lf_withidx.collect().shape)

        # Append row_id and compute end_time for queries 
        # IMPORTANT: filter to compatible symbols (rest are marked with nans)
        # Then convert from querys' to backends' enum type
        # From now on, query-lf only contains backend-compatible symbols. Nans are restored upon later join
        query_with_both = (
            query_lf_withidx.select('symbol', 'row_id', start_time_expr.alias('start_time'))
            .sort(['symbol', 'start_time'])
            .filter(pl.col('symbol').cast(str).is_in(list(self.db_symbol_enum.categories)))
            .with_columns(pl.col('symbol').cast(str).cast(self.db_symbol_enum))
            .with_columns(
                pl.col('start_time').dt.offset_by(mark_duration).alias('end_time')
            )
            .with_columns([
                pl.col('start_time').set_sorted(),
                pl.col('end_time').set_sorted()
            ])
        )
        # print(f'Query_with_both: {query_with_both.collect().shape}, {query_with_both.collect_schema()}')

        # Select a slice of the database containing these dates
        # Sort by tick_time
        min_date, max_date = query_with_both.select(
            pl.col('start_time').dt.date().min().alias('min'),
            pl.col('end_time').dt.date().max().alias('max')
        ).collect().row(0)

        inrange_db = self.db.filter(
                pl.col('date').is_between(
                    min_date, 
                    max_date
                )
            ).drop('date').sort(
                'symbol', 'tick_time'
            ).with_columns(pl.col('tick_time').set_sorted())
        # print(f'inrange_db: {inrange_db.collect_schema()}')

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
        # print(f'long_query: {long_query.collect().shape}, {long_query.collect_schema()}')

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
        ).select('row_id', 'symbol', 'query_time', 'query_type', 'tick_to_query_lag', 'fair')
        # print(f'joined_lf: {joined_lf.collect().shape}, {joined_lf.collect_schema()}')

        # Compute the columns to append
        append_cols = (
            joined_lf.group_by(['row_id', 'symbol'])
            .agg([
                pl.col('tick_to_query_lag').max().alias('max_tick_to_query_lag'), # This is maximum over ticks
                # pl.col('query_time').filter(pl.col('query_type') == 'start').first().alias('start_query_time'), 
                # pl.col('query_time').filter(pl.col('query_type') == 'end').first().alias('end_query_time'), 
                # pl.col('fair').filter(pl.col('query_type') == 'start').first().alias('start_fair'), 
                # pl.col('fair').filter(pl.col('query_type') == 'end').first().alias('end_fair'), 
                (
                    (pl.col('fair').filter(pl.col('query_type') == 'end').first() - 
                    pl.col('fair').filter(pl.col('query_type') == 'start').first()) 
                    / pl.col('fair').filter(pl.col('query_type') == 'start').first()
                ).alias('return')
            ])
            .sort('row_id')
        ).drop('symbol')
        # print(f'append_cols: {append_cols.collect().shape}, {append_cols.collect_schema()}')

        result = query_lf_withidx.join(
            append_cols, on='row_id', 
            how='left'
        ).drop('row_id')
        

        return result