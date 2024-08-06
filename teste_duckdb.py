import duckdb

# Aqui configuramos a opcao de spilling to disk, para processar arquivos maiores do que a memoria
con = duckdb.connect(config={"temp_directory": "./temp_dir.tmp/"})

# Primeiro, identificamos o produto mais vendido em termos de quantidade e canal
# Fazemos a soma de vendas (unidades) por tipo de produto e canal, e depois pegamos o maior produto
# por canal
maiores_vendas_por_canal = con.sql("""
        WITH cte AS (
          SELECT
            "Item Type", "Sales Channel", SUM("Units Sold") AS sum_units_sold
          FROM
            './vendas.csv'
          GROUP BY
            ALL
        )
        (
        SELECT
          *
        FROM
          cte
        WHERE
          "Sales Channel" = 'Online'
        ORDER BY
          sum_units_sold DESC
        LIMIT
          1
        )
        UNION BY NAME
        (
        SELECT
          *
        FROM
          cte
        WHERE
          "Sales Channel" = 'Offline'
        ORDER BY
          sum_units_sold DESC
        LIMIT
          1
        )
        """)

# Aqui determinamos qual pais e região teve o maior volume de vendas (em valor)
# Para isso, usamos a soma de renda total, agrupando por pais e regiao
maior_renda_por_pais_regiao = con.sql("""
        SELECT
          "Country", "Region", SUM("Total Revenue") AS sum_total_rev
        FROM
          './vendas.csv'
        GROUP BY
          ALL
        ORDER BY
          sum_total_rev DESC
        LIMIT
          1
        """)

# Nesse ultimo calculo, calculamos a média de vendas mensais por produto, considerando o período dos dados disponíveis.
# Primeiro, somamos o numero de meses em cada ano e pra cada tipo de produto. Importante esse passo pois em 2020 tivemos apenas
# 9 meses do ano registrados, entao a media deve ser calculada de acordo com esses meses apenas, nao 12.
# E, por fim, fazemos a media mensal considerando os anos do periodo.
media_mensal = con.sql("""
        WITH cte AS (
          SELECT
            "Item Type", YEAR("Order Date") AS year, SUM("Units Sold") / COUNT(DISTINCT MONTH("Order Date")) AS yearly_mean
          FROM
            './vendas.csv'
          GROUP BY
            "Item Type", year
        )
        SELECT
          "Item Type", SUM(yearly_mean) / COUNT(year) AS monthly_mean
        FROM
          cte
        GROUP BY
          "Item Type"
        """)

print(maiores_vendas_por_canal)
print(maior_renda_por_pais_regiao)
print(media_mensal)
