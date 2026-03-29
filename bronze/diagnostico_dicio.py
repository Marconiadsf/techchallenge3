"""
diagnostico_meses.py
────────────────────
Roda ANTES do concat_microdados.py quando você tiver os 3 CSVs.
Compara as colunas dos três meses e gera um relatório de diferenças.
Identifica os dois riscos reais:
  1. Colunas existentes em alguns meses mas não em outros (novas/removidas)
  2. Colunas com nomes diferentes que representam a mesma variável (renomeadas)
"""

import os
import sys
import pandas as pd


ARQUIVOS = {
    9:  "PNAD_COVID_092020.csv",
    10: "PNAD_COVID_102020.csv",
    11: "PNAD_COVID_112020.csv",
}


def detectar_separador(filepath):
    try:
        with open(filepath, "r", encoding="latin1") as f:
            amostra = f.read(5000)
        return "," if amostra.count(",") >= amostra.count(";") else ";"
    except Exception:
        return ","


def detectar_encoding(filepath):
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            with open(filepath, "r", encoding=enc) as f:
                f.read(2000)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin1"


# ── Ler apenas o cabeçalho de cada arquivo ────────────────────────────────
colunas_por_mes = {}
for mes, arq in ARQUIVOS.items():
    if not os.path.exists(arq):
        print(f"AVISO: {arq} não encontrado — pulando mês {mes}")
        continue
    sep = detectar_separador(arq)
    enc = detectar_encoding(arq)
    df_head = pd.read_csv(arq, sep=sep, encoding=enc, nrows=0)
    colunas_por_mes[mes] = set(df_head.columns)
    print(f"Mês {mes}: {len(df_head.columns)} colunas lidas de {arq}")

if len(colunas_por_mes) < 2:
    print("\nPrecisa de pelo menos 2 arquivos para comparar.")
    sys.exit(1)

meses = sorted(colunas_por_mes.keys())
print()

# ── Análise de diferenças ─────────────────────────────────────────────────
todas = set().union(*colunas_por_mes.values())
em_todos = set.intersection(*colunas_por_mes.values())

print("=" * 60)
print(f"Colunas presentes em TODOS os meses:  {len(em_todos)}")
print(f"União total de colunas:               {len(todas)}")
print(f"Colunas que diferem entre meses:      {len(todas) - len(em_todos)}")
print()

# Colunas exclusivas de cada mês
for mes in meses:
    exclusivas = colunas_por_mes[mes] - em_todos
    if exclusivas:
        print(f"Colunas EXCLUSIVAS do mês {mes} ({len(exclusivas)}):")
        for c in sorted(exclusivas):
            print(f"  + {c}")
    else:
        print(f"Mês {mes}: nenhuma coluna exclusiva")

print()

# Colunas ausentes em cada mês
for mes in meses:
    ausentes = todas - colunas_por_mes[mes]
    if ausentes:
        print(f"Colunas AUSENTES no mês {mes} ({len(ausentes)}):")
        for c in sorted(ausentes):
            presente_em = [m for m in meses if c in colunas_por_mes.get(m, set())]
            print(f"  - {c}  (existe em meses: {presente_em})")
    else:
        print(f"Mês {mes}: sem colunas ausentes")

print()

# ── Detecção de possíveis renomeações ─────────────────────────────────────
# Heurística: coluna exclusiva de um mês com nome muito similar a outra
print("=" * 60)
print("VERIFICAÇÃO DE POSSÍVEIS RENOMEAÇÕES")
print("(colunas com prefixo idêntico mas sufixo diferente entre meses)")
print()

from itertools import combinations

exclusivas_por_mes = {
    mes: colunas_por_mes[mes] - em_todos
    for mes in meses
    if mes in colunas_por_mes
}

renomeacoes_suspeitas = []
for mes_a, mes_b in combinations(meses, 2):
    exc_a = exclusivas_por_mes.get(mes_a, set())
    exc_b = exclusivas_por_mes.get(mes_b, set())
    for ca in exc_a:
        for cb in exc_b:
            # Mesmos primeiros 4 chars = suspeito de renomeação
            if ca[:4].upper() == cb[:4].upper() and ca != cb:
                renomeacoes_suspeitas.append((mes_a, ca, mes_b, cb))

if renomeacoes_suspeitas:
    print("ATENÇÃO — possíveis renomeações detectadas:")
    for ma, ca, mb, cb in renomeacoes_suspeitas:
        print(f"  Mês {ma}: {ca}  ↔  Mês {mb}: {cb}")
    print()
    print("  Se confirmado, o concat vai criar DUAS colunas separadas.")
    print("  Solução: renomear manualmente antes do concat.")
    print("  Exemplo no concat_microdados.py:")
    print("    df_9 = df_9.rename(columns={'NOME_ANTIGO': 'NOME_NOVO'})")
else:
    print("Nenhuma renomeação suspeita detectada. Seguro para concat.")

print()

# ── Resumo final e recomendação ───────────────────────────────────────────
print("=" * 60)
print("RESUMO E RECOMENDAÇÃO")
print()

novas_cols = todas - em_todos
if novas_cols:
    print(f"Existem {len(novas_cols)} colunas que não estão em todos os meses.")
    print("O concat vai preenchê-las com NaN onde ausentes — comportamento CORRETO.")
    print("No SQL Silver, o SAFE_CAST dessas colunas retorna NULL automaticamente.")
    print()
    print("Colunas afetadas (terão NULL em alguns meses):")
    for c in sorted(novas_cols):
        presente_em = [m for m in meses if c in colunas_por_mes.get(m, set())]
        print(f"  {c}  →  presente nos meses {presente_em}")
else:
    print("Todos os meses têm exatamente as mesmas colunas. Sem ação necessária.")

print()
print("Próximo passo: rodar concat_microdados.py")
