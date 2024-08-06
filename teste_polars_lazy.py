import polars as pl

df = pl.scan_csv("./vendas.csv")

# Primeiro, identificamos o produto mais vendido em termos de quantidade e canal
# Fazemos a soma de vendas (unidades) por tipo de produto e canal, e depois pegamos o maior produto
# por canal usando a funcao maior_venda_por_canal
cte = df.group_by(["Item Type", "Sales Channel"]).agg(
    pl.col("Units Sold").sum().alias("sum_units_sold")
)


def maior_venda_por_canal(canal):
    return (
        cte.filter(pl.col("Sales Channel") == canal)
        .sort("sum_units_sold", descending=True)
        .limit(1)
    ).collect(streaming=True)


# Calculamos aqui os produtos por canal
maiores_vendas_online = maior_venda_por_canal("Online")
maiores_vendas_offline = maior_venda_por_canal("Offline")

# Agregamos os resultados
maiores_vendas_por_canal = pl.concat([maiores_vendas_online, maiores_vendas_offline])

# Aqui determinamos qual pais e região teve o maior volume de vendas (em valor)
# Para isso, usamos a soma de renda total, agrupando por pais e regiao
maior_renda_por_pais_regiao = (
    df.group_by(["Country", "Region"])
    .agg(pl.col("Total Revenue").sum().alias("sum_total_rev"))
    .sort("sum_total_rev", descending=True)
    .limit(1)
).collect(streaming=True)

# Nesse ultimo calculo, calculamos a média de vendas mensais por produto, considerando o período dos dados disponíveis.
# Primeiro, somamos o numero de meses em cada ano e pra cada tipo de produto. Importante esse passo pois em 2020 tivemos apenas
# 9 meses do ano registrados, entao a media deve ser calculada de acordo com esses meses apenas, nao 12.
# E, por fim, fazemos a media mensal considerando os anos do periodo.

# Fazemos primeiro a transformacao da data, para datetime, criando uma "nova" (transformada) coluna
df = df.with_columns(pl.col("Order Date").str.to_datetime("%m/%d/%Y"))

# Primeiros calculamos a media mensal em cada ano
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

# E agora calculamos a media mensal considerando o periodo todo de anos
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
).collect(streaming=True)

print(maiores_vendas_por_canal)
print(maior_renda_por_pais_regiao)
print(media_mensal)
