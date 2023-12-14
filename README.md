# PROJETO B3 + AIRFLOW

O projeto consiste em construir uma DAG para realizar uma atualização diária onde:
- Teremos que extrair os dados da B3 (https://www.b3.com.br/pt_br/institucional) dos últimos 5 anos e depositá-los na tabela 'stage';
- Montaremos um esquema estrela com algumas tabelas dimensões escolhidas e uma tabela fato;
- Realizaremos a carga de dados para estas tabelas dimensões e fato.

A DAG 'b3_retro' é de uso único. Ela será responsável por alimentar a tabela 'stage' com os dados dos últimos 5 anos.

A DAG 'b3_reload' é de uso diário. Ela será responsável por atualizar os dados diariamente, fazendo as devidas inserções nas tabelas dimensões e fatos.

Este projeto consiste em um trabalho acadêmico. Vale ressaltar que estou no começo do meu aprendizado, então poderá haver erros ou redundâncias no código.
