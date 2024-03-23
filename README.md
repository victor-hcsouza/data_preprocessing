🇧🇷🇧🇷
### Primeiro projeto Python orientado a objeto voltado para engenharia de dados ###

Autor -> Victor Hugo Carvalho de Souza

Tecnologias usadas:
- Python 3.10.12
- PySpark 3.4.1
- Airflow 2.7.3
- AWS S3

Esse projeto eh responsavel pelo o que?
- Projeto responsavel por automatizar leitura de arquivos parquet da AWS S3 da zona Raw do datalake e evolui-los com calculos e tratamentos
- a fim de disponibilizar os dados curados para a area de negocio.

Qual o objetivo dessa automacao?
- Minimizar esforcos para criar visoes analiticas para a area de negocio, simplificar processo de leitura e escrita de dados, dar facilidade de manuntencao,
- aumentar confiabilidade no processo, padronizar processos de engenharia de dados e
- servir como template para processos que leem dados da raw -> curated mas tambem curated -> curated, curated -> analytics e analytics -> analytics.

Como eh o fluxo dessa automacao?
- Nesse projeto, o processo se inicia a partir da leitura de dados da zona Raw do datalake hospedado na AWS S3 e transforma-los em um dataframe. Apos isso,
- eh executada uma query SQL que faz todo tratamento e limpeza de dados para estarem prontos para a proxima camada no datalake. Em seguida, eh feito
- uma verificacao no dataframe curado usando Great Expectations e caso nao haja falhas, os dados estao prontos para serem salvos na nova zona.

🇺🇸🇺🇸
### First object-oriented Python project aimed at data engineering ###

Author -> Victor Hugo Carvalho de Souza

Technologies used:
- Python 3.10.12
- PySpark 3.4.1
- Airflow 2.7.3
- AWS S3

What is this project responsible for?
- Project responsible for automating reading of AWS S3 parquet files from the Raw zone of the datalake and evolving them with calculations and treatments
- in order to make the curated data available to the business area.

What is the purpose of this automation?
- Minimize efforts to create analytical views for the business area, simplify the process of reading and writing data, provide ease of maintenance,
- increase process reliability, standardize data engineering processes and
- serve as a template for processes that read data from raw -> curated but also curated -> curated, curated -> analytics and analytics -> analytics.

How is the flow of this automation?
- In this project, the process starts by reading data from the Raw zone of the datalake hosted on AWS S3 and transforming it into a dataframe. After this,
- an SQL query is executed that performs all data processing and cleaning to be ready for the next layer in the datalake. Then it's done
- a check on the curated dataframe using Great Expectations and if there are no failures, the data is ready to be saved in the new zone.