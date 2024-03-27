# desafio_gb_case

para a parte de modelagem,segue o diagrama inicial proposto:
![ETL caso Boticário (2)](https://github.com/arteweyl/desafio_gb_case2/assets/63889308/723e1f23-966c-4468-b343-acbc6b4cff3f)

O apresentável pode ser encontrado <a href='https://docs.google.com/presentation/d/1kUykvOltBTiCQaP4y33rc_RaKouyXTz4QfDGAecC35E/edit#slide=id.g2c6259c4439_0_14'>aqui</a>:

Este link Contem A solução de um Case scenário para uma implementação real 
utilizando spark e gcp.


Para o  Case 2 Scenario Utilizamos Classes Criadas pra lidar com a manipulação de dados de diversos conectores.
Passando aqui por diversos exemplos, utilizando buckets, serviço SQL do GCP com uma instância Postgres e muito mais.

os itens 1,2,3,4 que tratam sobre ingestão e transformação, foram lidadas no arquivo <a href='https://github.com/arteweyl/desafio_gb_case2/blob/main/ingestion.py'>ingestion.py</a>.Vale lembrar que a base de dados de 2017 e 2018 veio com problemas.  sendo a 2017 na verdade uma duplicata da de 2019 e a de 2018 sendo a de 2017,fiz validações de assert no executável do ingestion. Segue as imagens do problema com as bases:
![Screenshot_2](https://github.com/arteweyl/desafio_gb_case2/assets/63889308/40b7cbd4-902a-46ec-b6c1-86dd8700c114)
![base_2019](https://github.com/arteweyl/desafio_gb_case2/assets/63889308/adc02341-04d6-4f9d-812f-a25ccc7c5253)
![Base_2018_incongluencia](https://github.com/arteweyl/desafio_gb_case2/assets/63889308/3225e864-a26a-43d7-be3b-2ccd7e900ee9)


a segunda parte do case 2 que envolvia manipulação da API, pode ser vista diretamente no arquivo <a href='https://github.com/arteweyl/desafio_gb_case2/blob/main/spotify_pandas.py'>spotify_pandas.py</a>, ele contem todas as resoluções pedidas. Porem fiz também uma versão do que seria um "conector",  com essa api do spotify, chame de <a href=''>spotify_connector. para o primeiro item, um dos itens que é pedido no dataframe é o total_episodes, que não faz sentido ser colocado no dataframe, visto que é um item consolidado, essa informação ficaria sendo repedida.
a segunda parte do case 2 que envolvia manipulação da API, pode ser vista diretamente no arquivo spotify_pandas.py, ele contem todas as resoluções pedidas. Porem fiz também uma versão do que seria um "conector",  com essa api do spotify, chame de <a href='https://github.com/arteweyl/desafio_gb_case2/blob/main/ingestion/connectors/spotify_connector'>spotify_connector</a>. para o primeiro item, um dos itens que é pedido no dataframe é o total_episodes, que não faz sentido ser colocado no dataframe, visto que é um item consolidado, essa informação ficaria sendo repedida.
