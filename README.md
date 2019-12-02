## SEMANTIX-SPARK

## Intro

Script que lê e trabalha em cima de um determinado dataset.

## Tecnologias

Linguagem principal - Java
Compilador - Maven
Biblioteca - Spark-core - https://spark.apache.org/

Bibliotecas auxiliares:
-   **exec-maven-plugin** - utlizado em dev para executar sem jar

## Comandos
-  **mvn exec:java**
-  **mvn compile**
-  **mvn package**
-  **java -jar ...**

## Dataset
Na pasta raiz se encontra os dados zipados.
Os logs estão em arquivos ASCII com uma linha por requisição com as seguintes colunas:
- **Host fazendo a requisição**. Um hostname quando possível, caso contrário o endereço de internet se o nome
não puder ser identificado.
- **Timestamp** no formato "DIA/MÊS/ANO:HH:MM:SS TIMEZONE"
- **Requisição (entre aspas)**
- **Código do retorno HTTP**
- **Total de bytes retornado**

## Questionário do dataset

O questionário se encontra respondido no arquivo "re_dados.txt"