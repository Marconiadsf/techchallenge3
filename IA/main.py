import pandas as pd

# Lê só as primeiras 50.000 linhas (evita carregar tudo na memória)
df = pd.read_csv(
    "PNAD_COVID_112020.csv",
    sep=",",
    nrows=50000,
    encoding="latin1"  # arquivos IBGE geralmente são latin1
)

print(df.shape)
print(df.dtypes)
df.to_csv("amostra_pnad_50k.csv", sep=",", index=False)
print("Arquivo salvo! Tamanho aproximado:")
import os
print(f"{os.path.getsize('amostra_pnad_50k.csv') / 1e6:.1f} MB")
