# Utilização de dados do Sistema Único de Saúde (DATASUS) para previsão de pacientes de alto custo no ambiente hospitalar

Projeto que coleta dados do SIH (Sistema de informações hospitalares) para classificar quais pacientes são de "Alto custo".

### O que é Alto Custo?

São pacientes que, por conta da sua alta complexidade do estádo de saúde associada a condições crônicas e tratamento complexos, utilizam grande parte dos recursos financeiros de um serviço de saúde. A problemática não fica na questão financeira, mas sim no que esse valor representa como reflexo da qualidade de vída daquele paciente.

### Porque classificar e prever pacientes de Alto Custo?

É estimado que as principais doenças crônicas no Brasil geraram um custo de R$3,45
bilhões ao SUS no ano de 2018, onde desse total, 59% foram investidos no tratamento da
hipertensão, 30% na diabetes e 11% no tratamento da obesidade. [Nilson EAF
et.al (2018)](https://doi.org/10.26633/RPSP.2020.32)

Nesse contexto, além do alto custo para realização do tratamento, pacientes crônicos
possuem baixa adesão ao tratamento farmacológico, onde a prevalência da baixa adesão ao tratamento farmacológico de doenças crônicas foi de 30,8%. A complexidade do tratamento, associada ao alto custo, gera complicações no tratamento e piora da qualidade de vida da população. [Tavares et.al (2016)](https://www.scielo.br/j/rsp/a/R8pG5F3d3Qwx5Xz7dt6K6nx/?format=pdf)

Portanto, prever a probabilidade de um indivíduo se tornar um paciente de alto custo hospitalar no futuro também é poder ser ativo no cuidado de pacientes crônicos

### Arquitetura do projeto

![arquitetura](docs/[GitHub]%20Arch%20-%20alto%20custo%20sus.jpg)

1. Pipeline de ELT: 
     - Extração realizada com utilização da biblioteca do [PySUS](https://github.com/AlertaDengue/PySUS) e os dados brutos extraídos são salvos no AWS s3.
     - É utilizado o PySpark transformações e finalmente o novo dado é salvo no AWS DynamoDB.

2. Pipeline de Machine Learning:
     - O modelo é treinado com a chegada de novos dados.
     - O seu registro é feito no S3, salvando tanto o modelo serializado quanto metadados.
     - Todo novo dado dispara o ciclo de MLOps fazendo a inferência das métricas do modelo para os novos dados. Caso o modelo não esteja nos limites estabelecidos pelos testes, um novo modelo é treinado e substituido.