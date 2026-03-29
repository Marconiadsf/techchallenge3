"""
concat_microdados.py
Consolida os três CSVs da PNAD-COVID19 em um único arquivo.
Coloque este script na mesma pasta dos três CSVs e execute.
"""
 
import os
import sys
import pandas as pd
 
 
# ── configuração ──────────────────────────────────────────────────────────
ARQUIVOS = [
    "PNAD_COVID_092020.csv",
    "PNAD_COVID_102020.csv",
    "PNAD_COVID_112020.csv",
]
SAIDA = "pnad_covid_consolidado.csv"
 
 
# ── funções auxiliares ────────────────────────────────────────────────────
def detectar_separador(filepath):
    """
    Detecta o separador do CSV lendo os primeiros 5000 bytes.
    Retorna ',' ou ';'. Nunca retorna None.
    """
    try:
        with open(filepath, "r", encoding="latin1") as f:
            amostra = f.read(5000)
        n_virgulas = amostra.count(",")
        n_pv       = amostra.count(";")
        sep = "," if n_virgulas >= n_pv else ";"
        return sep
    except Exception as e:
        print(f"  Aviso: não foi possível detectar separador ({e}). Usando ','.")
        return ","
 
 
def detectar_encoding(filepath):
    """
    Tenta UTF-8 primeiro; cai para latin1 se falhar.
    Arquivos IBGE são geralmente latin1 nos CSVs mais antigos.
    """
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            with open(filepath, "r", encoding=enc) as f:
                f.read(2000)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin1"
 
 
# ── execução principal ────────────────────────────────────────────────────
dfs = []  # lista que vai acumular os DataFrames — NÃO reatribuir dentro do loop
 
for arq in ARQUIVOS:
 
    # Verificar se o arquivo existe antes de tentar abrir
    if not os.path.exists(arq):
        print(f"\nERRO: arquivo não encontrado: {arq}")
        print(f"  Pasta atual: {os.getcwd()}")
        print(f"  Arquivos nesta pasta: {os.listdir('.')}")
        sys.exit(1)
 
    sep = detectar_separador(arq)
    enc = detectar_encoding(arq)
 
    print(f"\nLendo: {arq}")
    print(f"  Separador detectado: '{sep}'  |  Encoding: {enc}")
 
    df_temp = pd.read_csv(arq, sep=sep, encoding=enc, low_memory=False)
 
    print(f"  Linhas: {len(df_temp):,}  |  Colunas: {len(df_temp.columns)}")
 
    # Verificar se V1013 (coluna de mês) existe
    if "V1013" in df_temp.columns:
        meses = sorted(df_temp["V1013"].unique())
        print(f"  Meses (V1013): {meses}")
    else:
        print(f"  Aviso: coluna V1013 não encontrada neste arquivo")
        print(f"  Colunas disponíveis: {list(df_temp.columns[:10])} ...")
 
    dfs.append(df_temp)  # ← append, nunca reatribuição
 
 
# Verificar que todos os três foram carregados
print(f"\nDataFrames carregados: {len(dfs)} de {len(ARQUIVOS)}")
assert len(dfs) == len(ARQUIVOS), "Nem todos os arquivos foram carregados"
 
# Concatenar — ignore_index reseta o índice para 0,1,2,...
# colunas extras do mes 11 ficam como NaN nos meses 9 e 10 (correto)
df_total = pd.concat(dfs, ignore_index=True)
 
# Verificações finais
print(f"\n=== Resultado da consolidação ===")
print(f"Total de linhas: {len(df_total):,}")
print(f"Total de colunas: {len(df_total.columns)}")
 
if "V1013" in df_total.columns:
    print(f"Meses presentes (V1013): {sorted(df_total['V1013'].unique())}")
    print(f"\nLinhas por mês:")
    print(df_total["V1013"].value_counts().sort_index().to_string())
else:
    print("AVISO: V1013 não encontrada no consolidado")
 
# Colunas que existem só no mês 11 (NaN nos outros meses)
total_cols = set(df_total.columns)
cols_mes9  = set(dfs[0].columns)
cols_mes10 = set(dfs[1].columns)
cols_mes11 = set(dfs[2].columns)
novas_mes11 = cols_mes11 - cols_mes9
if novas_mes11:
    print(f"\nColunas presentes só no mês 11 ({len(novas_mes11)}): {sorted(novas_mes11)}")
    print("  (NaN para meses 9 e 10 — comportamento correto)")
 
# Salvar
df_total.to_csv(SAIDA, index=False)
tamanho_mb = os.path.getsize(SAIDA) / 1e6
print(f"\n✓ Arquivo salvo: {SAIDA}  ({tamanho_mb:.0f} MB)")
print("Pronto para upload no BigQuery.")
