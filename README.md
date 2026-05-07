# 🔥 Spark Challenge — O Pipeline que Trava no TikTok

> Nível: Mid → Senior | Tempo estimado: 2–4 horas | Stack: PySpark 3.x

---

## Contexto de Negócio

Você acabou de entrar na equipe de dados da **Shoppa**, um marketplace brasileiro com 8 milhões de usuários ativos.

O time de produto quer um **dashboard de sessões por categoria de produto** — quantas sessões por dia, qual o ticket médio, qual o funil de conversão (view → cart → purchase).

O time anterior deixou um pipeline que "funciona", mas o job de Spark trava ou demora horas quando algum produto viraliza no TikTok. Seu trabalho é entender por que isso acontece e consertar.

---

## Dataset

Dois datasets no diretório `data/`:

### `clickstream/` — Eventos de comportamento do usuário


| Coluna       | Tipo      | Descrição                                     |
| ------------ | --------- | --------------------------------------------- |
| `event_id`   | string    | UUID do evento                                |
| `session_id` | string    | ID da sessão do usuário                       |
| `user_id`    | string    | ID do usuário                                 |
| `product_id` | string    | ID do produto clicado                         |
| `event_type` | string    | `view`, `add_to_cart`, `purchase`             |
| `event_ts`   | timestamp | Timestamp do evento                           |
| `revenue`    | double    | Receita (só para `purchase`, null nos demais) |


### `products/` — Catálogo de produtos


| Coluna         | Tipo   | Descrição                           |
| -------------- | ------ | ----------------------------------- |
| `product_id`   | string | ID do produto                       |
| `product_name` | string | Nome do produto                     |
| `category`     | string | Categoria (Eletrônicos, Moda, etc.) |
| `price`        | double | Preço base                          |
| `seller_id`    | string | ID do vendedor                      |


**Volume:** ~5 milhões de eventos, 10.000 produtos.

---

## Tasks

### Task 1 — Carregar e Perfilar

Carregue os dois datasets e responda:

1. Quantas linhas tem cada dataset?
2. Qual é a distribuição de `event_type`?
3. Qual produto tem mais eventos? Quantos eventos ele representa do total (em %)?
4. Plote (ou imprima) o top 10 produtos por volume de eventos.

> **Entregável:** Script + prints dos resultados.
> **O que estamos avaliando:** Você consegue identificar o skew antes de escrever qualquer join?

---

### Task 2 — Join Naive e Diagnóstico

Escreva o join mais simples possível entre `clickstream` e `products` para calcular:

- Total de sessões por categoria por dia
- Receita total por categoria por dia
- Taxa de conversão por categoria (purchases / views)

```python
# Estrutura esperada do output
# +----------+------------+--------+---------+------------------+
# |categoria |data        |sessoes |receita  |taxa_conversao    |
# +----------+------------+--------+---------+------------------+
```

Depois de rodar, responda:

1. Quanto tempo demorou?
2. Abra o Spark UI (ou observe os logs). Qual stage está lento? Por quê?
3. Como você confirmaria que é skew e não outra coisa?

> **Entregável:** Script + análise escrita de 3–5 linhas explicando o diagnóstico.
> **O que estamos avaliando:** Você sabe ler o Spark UI? Consegue articular a causa raiz?

---

### Task 3 — Fix com Broadcast Join

O catálogo de produtos (`products`) tem 10.000 linhas — cabe fácil em memória de cada executor.

1. Reescreva o join usando **broadcast join**
2. Meça o tempo novamente
3. Explique em 2–3 linhas por que isso funciona neste caso

> **Atenção:** Qual o limite de tamanho que você usaria como regra geral para decidir entre broadcast e sort-merge join? Justifique.

> **Entregável:** Script refatorado + análise comparativa de tempo + resposta à pergunta.

---

### Task 4 — Fix com Salting (cenário alternativo)

Imagine que o catálogo cresceu e agora tem **50 milhões de produtos** — broadcast não é mais uma opção.

Implemente a técnica de **salting** para resolver o skew no sort-merge join:

1. Adicione uma coluna `salt` (inteiro aleatório de 0 a N) no clickstream
2. Exploda o dataset de produtos para cada valor de salt
3. Faça o join em `(product_id, salt)`
4. Agregue normalmente

Responda:

- Qual o valor ideal de N (número de salts)? Como você escolheria em produção?
- Qual o tradeoff de aumentar muito N?

> **Entregável:** Script com salting + respostas às perguntas.
> **O que estamos avaliando:** Você entende o mecanismo, não só o código.

---

### Task 5 — Output e Teste Unitário

1. Salve o resultado final como **Delta table** (ou Parquet se não tiver Delta) particionado por `data`
2. Escreva **1 teste unitário** para a função de transformação principal (a que calcula taxa de conversão)

Para o teste:

- Use um DataFrame pequeno criado na mão (`spark.createDataFrame`)
- Valide o schema e pelo menos 1 valor calculado
- Use `pytest` ou `unittest`

> **Entregável:** Script de escrita + arquivo de teste.
> **O que estamos avaliando:** Você escreve código testável? O teste testa comportamento, não implementação?

---

## Como Entregar

- Um repositório Git (público ou compartilhado) com:
  ```
  ├── src/
  │   ├── task1_profile.py
  │   ├── task2_naive_join.py
  │   ├── task3_broadcast.py
  │   ├── task4_salting.py
  │   └── task5_output.py
  ├── tests/
  │   └── test_transformations.py
  └── ANALYSIS.md   ← suas respostas às perguntas abertas
  ```

---

## Critério de Avaliação (transparente)


| Critério                                                             | Peso |
| -------------------------------------------------------------------- | ---- |
| Identifica o skew corretamente (Task 1)                              | 20%  |
| Diagnóstico articulado do problema (Task 2)                          | 20%  |
| Implementação correta do broadcast join (Task 3)                     | 20%  |
| Implementação correta do salting + explicação dos tradeoffs (Task 4) | 25%  |
| Qualidade do teste unitário (Task 5)                                 | 15%  |


> O código que funciona vale menos do que o código que funciona **e** explica por que funciona.

---

## Setup

```bash
# Gerar os dados de teste
python seed_data.py

# Rodar os scripts
spark-submit src/task1_profile.py

# Rodar os testes
pytest tests/
```

**Requisitos:** Python 3.11+, PySpark 3.4+, pytest