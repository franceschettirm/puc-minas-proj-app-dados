# Visão geral
Este repositório foi criado para hospedar os códigos relativos à aplicação de banco de dados desenvolvida para o projeto do primeiro semestre do curso de tecnólogo em banco de dados da PUC Minas.

## Objetivo
Desenvolver uma aplicação que consome arquivos CSV contendo dados de combustíveis (preço, bandeira de venda, região geográfica da venda, estado etc.), realiza alguns tratamentos e ingere três tabelas em um banco de dados de código aberto (PostgreSQL). 

## Fluxo dos dados
As três tabelas são uma "source of record", que consiste nos dados CSV puros; uma "source of truth" que consiste em alguns tratamentos básicos (em especial, remoção de valores faltantes e de colunas que não são objetos do estudo); e uma especializada, com todos os tratamentos necessários para a visualização final.

