## Digital College - Python para Dados: Turma 01
# Pipeline, Engenharia e Análise dos Dados da Qualidade da Água no Estado do Ceará entre 2020 e 2025 via API SISAGUA/Dados Abertos
#### Trabalho de conclusão da primeira turma do curso Python para Dados da Digital College, com objetivo de trabalhar conteúdos abordados no decorrer dos módulos, sendo desenvolvido todo o processo de pipeline de dados, incrementado pela etapa de engenharia de dados, como a criação da infraestrutura para viabilidade da automatização do pipeline a partir da criação de servidores, orquestradores de API e banco de dados, sendo o processo finalizado com as análises exploratória e com Machine Learning dos dados para obtenção de classificações e predições de amostras a partir das features determinadas.
------------------------------------------------------------------------------------------------------------------------------------------------------------
## O que é o SISAGUA?
💦 É o Sistema de Informação para vigilância da qualidade da água , que tem por finalidade monitorar água, apoiar políticas públicas e vigilância. Tem como principais parâmetros
- 🔬 Microbiológico
- 🧪 Físico-químico
- 😖 Organolépticos

Sua base é alimentada por Concessionárias de Água, pela Vigilância em Saúde Ambiental e Secretarias municipais ou estaduais.
>>>>>>>>>>----------
## Pipeline de Dados
⛏️ Extração: consumo da API SISAGUA para obtenção dos dados brutos;

⏳ Orquestração: com Apache Airflow;

🐍 Transformação: limpeza e padronização dos dados utilizando bibliotecas Python;

💾 Armazenamento: uso do ecossistema Hadoop para armazenar dados tratados de forma distribuída.

>>>>>>>>>>----------

## Engenharia de Dados
🐧 Ubuntu Server como servidor;

🪞 XShell como clone do Ubuntu Server para facilitar e aproveitar recursos;

𖣘 Airflow como orquestrador da etapa de varredura dos dados a partir da API;

🐘 Hadoop como framework de armazenamento e distribuição entre nós (no cado só dois) dos dados capturados;

🗃️ WinSCP como interface gráfica de comunicação entre o Windows e os diretórios do Ubuntu Server

🐋 Docker como sistema de virtualização para receber e executar o PostgreSQL

🐘 PostgreSQL como banco de dados para armazenamento local dos dados que possibilitou melhor performance na leitura dos dados.

>>>>>>>>>>----------

## Análise dos Dados e Machine Learning
🕵️‍♂️ Análise exploratória básica;

📈 Estatísticas descritivas dos parâmetros de qualidade da água;

📊 Visualização de gráficos;

💡 Insights a partir de questionamentos que surgiram nas análises preliminares;

🙅‍♂️ Identificação de dados viciados, preenchimento equivocado e faltantes;

🦾 Machine Learning - Modelos classificatórios
  - Random Forest Classifier
  - XGBoost
  - Regressão Logística
  - Regressão Linear
------------------------------------------------------------------------------------------------------------------------------------------------------------
⚙️ _Requisitos_

Python 3.12

Apache Airflow 2.9.0

Hadoop HDFS 3.3.6

Vitrual Box 7.0

Xshell 8.0.0063p

Ubuntu Server 24.04.1

WinSCP 6.3.6

PostgreSQL 16

Docker 27.4.0
