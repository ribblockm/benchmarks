# Nao comentei esse codigo pois nao considero-o para submissao, apenas como comparacao nos benchmarks

import polars as pl

df = pl.read_csv("./vendas.csv")

cte = df.group_by(["Item Type", "Sales Channel"]).agg(
    pl.col("Units Sold").sum().alias("sum_units_sold")
)


def maior_venda_por_canal(canal):
    return (
        cte.filter(pl.col("Sales Channel") == canal)
        .sort("sum_units_sold", descending=True)
        .limit(1)
    )


maior_renda_por_pais_regiao = (
    df.group_by(["Country", "Region"])
    .agg(pl.col("Total Revenue").sum().alias("sum_total_rev"))
    .sort("sum_total_rev", descending=True)
    .limit(1)
)

df = df.with_columns(pl.col("Order Date").str.to_datetime("%m/%d/%Y"))

cte2 = (
    df.group_by(["Item Type", pl.col("Order Date").dt.year().alias("year")])
    .agg(
        [
            pl.col("Units Sold").sum().alias("total_units"),
            pl.col("Order Date").dt.month().n_unique().alias("distinct_months"),
        ]
    )
    .with_columns(
        (pl.col("total_units") / pl.col("distinct_months")).alias("yearly_mean")
    )
)

media_mensal = (
    cte2.group_by("Item Type")
    .agg(
        [
            pl.col("yearly_mean").sum().alias("sum_yearly_mean"),
            pl.col("year").count().alias("count_years"),
        ]
    )
    .with_columns(
        (pl.col("sum_yearly_mean") / pl.col("count_years")).alias("monthly_mean")
    )
    .select(["Item Type", "monthly_mean"])
)

maiores_vendas_online = maior_venda_por_canal("Online")
maiores_vendas_offline = maior_venda_por_canal("Offline")

maiores_vendas_por_canal = pl.concat([maiores_vendas_online, maiores_vendas_offline])

print(maiores_vendas_por_canal)
print(maior_renda_por_pais_regiao)
print(media_mensal)
