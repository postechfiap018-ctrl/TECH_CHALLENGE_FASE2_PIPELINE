# Evidências de execução

Este documento reúne evidências de que a pipeline rodou de ponta a ponta contra a **conta AWS
real** (`247670276488`, região `us-east-2`), extraídas diretamente via AWS CLI/Athena — não são
capturas manuais, são consultas ao vivo ao histórico de execução da conta.

Para as capturas de tela do Console (complementares a este documento), ver a seção
["Como capturar prints/vídeo do Console"](#como-capturar-printsvídeo-do-console-aws) no fim.

## Histórico de execuções dos jobs Glue

**Job Silver** (`alfabetizacao-silver-job`) — últimas 5 execuções, todas `SUCCEEDED`:

| Execução (Id abreviado) | Início | Fim | Duração |
|---|---|---|---|
| `jr_af5dc8f9...` | 2026-08-28 22:49:27 -03 | 2026-08-28 22:51:32 -03 | 118s |
| `jr_cd743cd8...` | 2026-08-25 22:22:08 -03 | 2026-08-25 22:24:09 -03 | 114s |
| `jr_6ef98986...` | 2026-08-25 22:18:53 -03 | 2026-08-25 22:20:48 -03 | 105s |
| `jr_f60be50e...` | 2026-08-22 19:06:40 -03 | 2026-08-22 19:08:43 -03 | 111s |
| `jr_29f00e85...` | 2026-08-22 19:00:09 -03 | 2026-08-22 19:01:59 -03 | 104s |

**Job Gold** (`alfabetizacao-gold-job`) — últimas 5 execuções, todas `SUCCEEDED`:

| Execução (Id abreviado) | Início | Fim | Duração |
|---|---|---|---|
| `jr_03291152...` | 2026-08-28 22:51:41 -03 | 2026-08-28 22:52:58 -03 | 73s |
| `jr_40cb9b49...` | 2026-08-22 19:08:56 -03 | 2026-08-22 19:10:08 -03 | 68s |
| `jr_e1bc81ef...` | 2026-08-22 19:02:02 -03 | 2026-08-22 19:03:06 -03 | 59s |
| `jr_2c79b560...` | 2026-08-22 18:56:04 -03 | 2026-08-22 18:57:37 -03 | 86s |
| `jr_2d777631...` | 2026-08-22 18:50:31 -03 | 2026-08-22 18:51:46 -03 | 71s |

Reproduzir: `aws glue get-job-runs --job-name alfabetizacao-silver-job --region us-east-2`

## Log estruturado da última execução (CloudWatch)

Extraído de `/aws-glue/jobs/output` — job `jr_af5dc8f9...` (Silver) seguido de `jr_03291152...`
(Gold), execução de 2026-08-28:

```
2026-08-29T01:51:22Z | INFO     | ============================================================
2026-08-29T01:51:22Z | INFO     | SUMARIO SILVER
2026-08-29T01:51:22Z | INFO     |   uf                          : 27 registros
2026-08-29T01:51:22Z | INFO     |   municipio                   : 5571 registros
2026-08-29T01:51:22Z | INFO     |   resultado_municipio         : 23995 registros
2026-08-29T01:51:22Z | INFO     |   resultado_uf                : 145 registros
2026-08-29T01:51:22Z | INFO     |   metas_brasil                : 7 registros
2026-08-29T01:51:22Z | INFO     |   metas_uf                    : 189 registros
2026-08-29T01:51:22Z | INFO     |   metas_municipio             : 37464 registros
2026-08-29T01:51:22Z | INFO     |   alunos                      : 704336 registros
2026-08-29T01:51:22Z | INFO     |   Proxima etapa: executar o job Gold
2026-08-29T01:51:22Z | INFO     | ============================================================
...
2026-08-29T01:52:47Z | INFO     | ============================================================
2026-08-29T01:52:47Z | INFO     | SUMARIO GOLD
2026-08-29T01:52:47Z | INFO     |   indicador_por_municipio     : 5500 registros
2026-08-29T01:52:47Z | INFO     |   comparacao_metas_resultados : 5352 registros
2026-08-29T01:52:47Z | INFO     |   evolucao_temporal_indicador : 51 registros
2026-08-29T01:52:47Z | INFO     | ============================================================
```

Reproduzir: `aws logs tail /aws-glue/jobs/output --since 3d --region us-east-2 --filter-pattern "SUMARIO"`

## Dados reais no S3 (as três camadas)

```
$ aws s3 ls s3://tech-challenge-alfabetizacao-aline/bronze/ --region us-east-2
PRE alunos/
PRE meta_alfabetizacao_brasil/
PRE meta_alfabetizacao_municipio/
PRE meta_alfabetizacao_uf/
PRE municipio/
PRE municipio_resultado_alfabetizacao/
PRE uf/
PRE uf_resultado_alfabetizacao/

$ aws s3 ls s3://tech-challenge-alfabetizacao-aline/silver/ --region us-east-2
PRE alunos/
PRE metas_brasil/
PRE metas_municipio/
PRE metas_uf/
PRE municipio/
PRE resultado_municipio/
PRE resultado_uf/
PRE uf/

$ aws s3 ls s3://tech-challenge-alfabetizacao-aline/gold/ --region us-east-2
PRE comparacao_metas_resultados/
PRE evolucao_temporal_indicador/
PRE indicador_por_municipio/

$ aws s3 ls s3://tech-challenge-alfabetizacao-aline/ --region us-east-2 --recursive --summarize
Total Objects: 194
Total Size: 31195041
```

## Consultas Athena contra a camada Gold (resultado real)

| Tabela | Linhas | Query ID (Athena) |
|---|---|---|
| `gold_indicador_por_municipio` | 5.500 | `8326e05f-f153-433a-94ed-960e4da4a5cc` |
| `gold_comparacao_metas_resultados` | 5.352 | `0b20541f-2968-4bd5-9668-a69cd55684fb` |
| `gold_evolucao_temporal_indicador` | 51 | `39ac54b8-3be9-424d-89a7-287c6edf0093` |

Qualquer Query ID acima pode ser conferido no Console: **Athena → Consultas recentes → colar o ID**.

## Como capturar prints/vídeo do Console AWS

Este documento cobre a evidência "de código" (CLI/logs/Athena). Complemente com capturas de
tela do Console — mais fácil pro professor reconhecer visualmente. Sugestão de 5 prints, nessa
ordem:

1. **S3** → bucket `tech-challenge-alfabetizacao-aline` → mostrar as pastas `bronze/`, `silver/`,
   `gold/` abertas.
2. **Glue → Jobs** → `alfabetizacao-silver-job` e `alfabetizacao-gold-job` → aba **"Runs"** →
   mostrar o histórico de execuções `Succeeded` (é a mesma tabela que está no topo deste
   documento, só que visual).
3. **CloudWatch → Log groups** → `/aws-glue/jobs/output` → abrir o log da última execução →
   mostrar o bloco `SUMARIO SILVER`/`SUMARIO GOLD` (o mesmo texto que está acima).
4. **Athena → Editor de consultas** → rodar `SELECT * FROM gold_evolucao_temporal_indicador
   ORDER BY ano` → print do resultado.
5. **CloudWatch → Alarms** → mostrar os alarmes configurados (mesmo que "OK", mostra que o
   monitoramento está no ar).

Para o **vídeo** (pode ser o mesmo vídeo executivo ou um trecho à parte, sem contar no tempo dos
5 minutos se for anexado separadamente): grave a tela rodando

```bash
python -m infra.provision_aws
```

ou disparando um job pelo Console (**Glue → Jobs → Run**) e acompanhando o status mudar para
`Running` → `Succeeded` em tempo real. Ferramentas simples: **Gravador de Tela do Windows**
(`Win + Alt + R` com uma janela ativa) ou o **Xbox Game Bar** (`Win + G`), que já vêm no
Windows — não precisa instalar nada.
